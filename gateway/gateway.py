"""网关编排：归一 -> 会话/配额 -> 缓存 -> 转发/补发，全程判定可追溯。

数据通路（确定性）：
1. 归一化失败 -> 拒绝（含原因与原始出处）；
2. 会话/配额校验失败 -> 拒绝，不影响其他设备；
3. 入缓存：重复 -> 幂等接受（duplicate=True）；满 -> BUFFER_FULL 拒绝；
4. 入缓存成功后尝试按序转发；下游不可用时保留在缓存，恢复后 flush 按序补发。

配置治理与追溯：
- ingest/connect/disconnect/flush 与配置更新共用注册表锁（RLock）：
  单条报文的判定要么完全用旧版本配置、要么完全用新版本，不会混用；
  配置切换与上报/补发并发时，补发顺序（offset）不受影响；
- 每个事件（判定/会话/补发）连同当时生效的配置版本写入有界台账，
  事后可按设备/时间段查询，并可按版本重放复现；
- 已受理记录的 config_version 随缓存记录流转：补发不再重新判定，
  配置后续变更不改变已受理数据的语义。
"""
from __future__ import annotations

import logging
import threading

from .audit import Decision, DecisionLedger, RecordKind
from .buffer import SpoolBuffer, StoreResult
from .downstream import Downstream
from .models import IngestResult, RawEnvelope, Rejection, RejectReason
from .normalizer import Normalizer
from .session import SessionManager

logger = logging.getLogger("gateway")


class Gateway:
    def __init__(
        self,
        normalizer: Normalizer,
        sessions: SessionManager,
        buffer: SpoolBuffer,
        downstream: Downstream,
        ledger: DecisionLedger | None = None,
    ) -> None:
        self._normalizer = normalizer
        self._sessions = sessions
        self._buffer = buffer
        self._downstream = downstream
        self._ledger = ledger
        # 与配置更新互斥：注册表锁（RLock，ingest 内可重入）；
        # 重放等无注册表场景用私有锁。
        registry = sessions.registry
        self._lock: threading.RLock = registry.lock if registry is not None else threading.RLock()

    @property
    def buffer(self) -> SpoolBuffer:
        return self._buffer

    @property
    def sessions(self) -> SessionManager:
        return self._sessions

    @property
    def ledger(self) -> DecisionLedger | None:
        return self._ledger

    @property
    def lock(self) -> threading.RLock:
        """判定路径与配置更新共用的互斥锁。"""
        return self._lock

    def _record(self, kind: RecordKind, device_id: str, decision: Decision,
                reason: str | None, config_version: int, received_at_ms: int,
                envelope: RawEnvelope, detail: str = "") -> None:
        if self._ledger is not None:
            self._ledger.append(kind, device_id, decision, reason, config_version,
                                received_at_ms, envelope, detail)

    def connect(self, device_id: str, received_at_ms: int = 0) -> RejectReason | None:
        """建立设备连接；结果（含并发配额拒绝）连同配置版本入台账。"""
        with self._lock:
            reason = self._sessions.open_connection(device_id)
            envelope = RawEnvelope(source="session", payload={"device_id": device_id},
                                   received_at_ms=received_at_ms)
            if reason is None:
                self._record(RecordKind.SESSION_OPEN, device_id, Decision.ACCEPTED, None,
                             self._sessions.config_version_of(device_id),
                             received_at_ms, envelope)
            else:
                self._record(RecordKind.SESSION_OPEN, device_id, Decision.REJECTED,
                             reason.value, self._sessions.config_version_of(device_id),
                             received_at_ms, envelope, "connect quota exceeded")
            return reason

    def disconnect(self, device_id: str, received_at_ms: int = 0) -> None:
        with self._lock:
            self._sessions.close_connection(device_id)
            envelope = RawEnvelope(source="session", payload={"device_id": device_id},
                                   received_at_ms=received_at_ms)
            self._record(RecordKind.SESSION_CLOSE, device_id, Decision.ACCEPTED, None,
                         self._sessions.config_version_of(device_id),
                         received_at_ms, envelope)

    def ingest(self, envelope: RawEnvelope) -> IngestResult:
        """接入单条原始报文，返回可区分的判定结果；判定连同配置版本入台账。"""
        with self._lock:
            return self._ingest_locked(envelope)

    def _ingest_locked(self, envelope: RawEnvelope) -> IngestResult:
        normalized = self._normalizer.normalize(envelope)
        if isinstance(normalized, Rejection):
            logger.info("reject %s: %s", normalized.reason.value, normalized.detail)
            # 归一化拒绝与设备配置无关；尽量取报文自称的设备，版本记其生效配置
            claimed = envelope.payload.get("device_id")
            device_id = claimed if isinstance(claimed, str) and claimed else "<unknown>"
            self._record(RecordKind.DECISION, device_id, Decision.REJECTED,
                         normalized.reason.value,
                         self._sessions.config_version_of(device_id if device_id != "<unknown>" else ""),
                         envelope.received_at_ms, envelope, normalized.detail)
            return IngestResult(accepted=False, rejection=normalized)
        point = normalized
        config_version = self._sessions.config_version_of(point.device_id)

        # 重复投递（内容完全相同，含已确认的历史报文）幂等接受，先于序列检查：
        # 上游重发旧报文不应计为序列回退，也不应重复补发。
        if self._buffer.is_duplicate(point):
            logger.info("duplicate device=%s metric=%s seq=%s (idempotent accept)",
                        point.device_id, point.metric, point.seq)
            self._record(RecordKind.DECISION, point.device_id, Decision.DUPLICATE, None,
                         config_version, envelope.received_at_ms, envelope,
                         "idempotent accept")
            return IngestResult(accepted=True, point=point, duplicate=True)

        # 先入缓存再做会话/配额判定，保证补发 offset 等于接收顺序；
        # 判定拒绝则回滚（offset 空洞不影响后续记录的顺序）。
        stored = self._buffer.store(point, config_version=config_version)
        if stored is StoreResult.FULL:
            rej = Rejection(reason=RejectReason.BUFFER_FULL, envelope=envelope,
                            detail=f"buffer capacity {self._buffer.capacity} exhausted")
            logger.info("reject %s: %s", rej.reason.value, rej.detail)
            self._record(RecordKind.DECISION, point.device_id, Decision.REJECTED,
                         rej.reason.value, config_version, envelope.received_at_ms,
                         envelope, rej.detail)
            return IngestResult(accepted=False, rejection=rej)

        reason = self._sessions.check(point)
        if reason is not None:
            self._buffer.rollback_last(point)
            rej = Rejection(reason=reason, envelope=envelope,
                            detail=f"device={point.device_id} seq={point.seq}")
            logger.info("reject %s: %s", rej.reason.value, rej.detail)
            self._record(RecordKind.DECISION, point.device_id, Decision.REJECTED,
                         reason.value, config_version, envelope.received_at_ms,
                         envelope, rej.detail)
            return IngestResult(accepted=False, rejection=rej)

        logger.info("stored device=%s metric=%s seq=%s config_version=%d",
                    point.device_id, point.metric, point.seq, config_version)
        self._record(RecordKind.DECISION, point.device_id, Decision.ACCEPTED, None,
                     config_version, envelope.received_at_ms, envelope)
        self._flush_locked()
        return IngestResult(accepted=True, point=point)

    def flush(self) -> int:
        """下游可用时按序补发缓存数据，返回补发条数。"""
        with self._lock:
            return self._flush_locked()

    def _flush_locked(self) -> int:
        sent = 0
        for rec in self._buffer.pending():
            if self._downstream.send(rec):
                self._buffer.ack_through(rec.offset)
                sent += 1
                logger.info("forwarded offset=%d device=%s seq=%d config_version=%d",
                            rec.offset, rec.point.device_id, rec.point.seq,
                            rec.config_version)
                # 补发流水入台账：版本是受理时的版本，补发不重新判定
                self._record(RecordKind.FORWARD, rec.point.device_id, Decision.ACCEPTED,
                             None, rec.config_version, rec.point.received_at_ms,
                             RawEnvelope(source=rec.point.source, payload={},
                                         received_at_ms=rec.point.received_at_ms),
                             f"offset={rec.offset} seq={rec.point.seq}")
            else:
                logger.info("downstream unavailable, %d record(s) remain buffered",
                            len(self._buffer))
                break
        return sent
