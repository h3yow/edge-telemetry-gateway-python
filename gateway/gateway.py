"""网关编排：归一 -> 会话/配额 -> 缓存 -> 转发/补发，全程配置钉版 + 判定留痕。

数据通路（确定性）：
1. 归一化失败 -> 拒绝（含原因与原始出处）；
2. 会话/配额校验失败 -> 拒绝，不影响其他设备；
3. 入缓存：重复 -> 幂等接受（duplicate=True）；满 -> BUFFER_FULL 拒绝；
4. 入缓存成功后尝试按序转发；下游不可用时保留在缓存，恢复后 flush 按序补发。

配置治理与追溯：
- 每次判定在锁内完成「取生效配置 -> 会话/配额判定 -> 留痕」三步，配置热更新
  不会插入单次判定中间，同一批数据不会被新旧两套配置重复判定；
- 每条判定记录钉住当时生效的配置版本与快照；已收下未转发的数据在受理时
  已完成判定，补发只按 offset 顺序转发，不再重新判定，因此改配置不会改变
  历史数据的结论，也不会打乱补发顺序；
- 判定日志有界（AuditLog 容量上限），超出淘汰最旧并计数。
"""
from __future__ import annotations

import logging
import threading

from .audit import AuditLog, Decision
from .buffer import SpoolBuffer, StoreResult
from .config import ConfigRegistry, ConfigUpdate, DeviceConfig, EffectiveConfig
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
        config_registry: ConfigRegistry | None = None,
        audit_log: AuditLog | None = None,
    ) -> None:
        self._normalizer = normalizer
        self._sessions = sessions
        self._buffer = buffer
        self._downstream = downstream
        # 未显式提供注册表时，用会话层的全局策略作为默认配置（向后兼容），
        # 并跟随 sessions.update_policy 的版本前进同步默认配置。
        self._owns_registry = config_registry is None
        if config_registry is None:
            p = sessions.policy
            config_registry = ConfigRegistry(
                DeviceConfig(p.max_concurrent_sessions_per_device,
                             p.max_reports_per_window, p.window_ms),
                version=p.version)
        self._configs = config_registry
        self._audit = audit_log if audit_log is not None else AuditLog()
        # 单次判定的三步（取配置/判定/留痕）在锁内完成，保证与热更新并发时确定。
        self._lock = threading.RLock()

    @property
    def buffer(self) -> SpoolBuffer:
        return self._buffer

    @property
    def sessions(self) -> SessionManager:
        return self._sessions

    @property
    def configs(self) -> ConfigRegistry:
        return self._configs

    @property
    def audit(self) -> AuditLog:
        return self._audit

    def _effective(self, device_id: str) -> EffectiveConfig:
        if self._owns_registry:
            # 兼容旧接口：sessions.update_policy 前进版本时，同步为注册表默认配置。
            p = self._sessions.policy
            if p.version > self._configs.version:
                self._configs.apply(ConfigUpdate(
                    version=p.version,
                    default=DeviceConfig(p.max_concurrent_sessions_per_device,
                                         p.max_reports_per_window, p.window_ms)))
        return self._configs.effective_for(device_id)

    def connect(self, device_id: str, at_ms: int = 0) -> RejectReason | None:
        """建立设备连接：按该设备生效配置的并发上限判定，并留痕。"""
        with self._lock:
            eff = self._effective(device_id)
            reason = self._sessions.open_connection(
                device_id, eff.config.as_policy(eff.version))
            self._audit.record_connect(device_id, reason, eff.version, eff.config, at_ms)
            return reason

    def disconnect(self, device_id: str, at_ms: int = 0) -> None:
        with self._lock:
            eff = self._effective(device_id)
            self._sessions.close_connection(device_id)
            self._audit.record_disconnect(device_id, eff.version, eff.config, at_ms)

    def ingest(self, envelope: RawEnvelope) -> IngestResult:
        """接入单条原始报文，返回可区分的判定结果；判定与配置版本一起留痕。"""
        with self._lock:
            normalized = self._normalizer.normalize(envelope)
            if isinstance(normalized, Rejection):
                logger.info("reject %s: %s", normalized.reason.value, normalized.detail)
                dev = envelope.payload.get("device_id")
                eff = self._effective(dev if isinstance(dev, str) else "")
                self._audit.record_decision(envelope, None, Decision.REJECTED,
                                            normalized.reason, eff.version, eff.config)
                return IngestResult(accepted=False, rejection=normalized)
            point = normalized
            eff = self._effective(point.device_id)
            policy = eff.config.as_policy(eff.version)

            # 重复投递（内容完全相同，含已确认的历史报文）幂等接受，先于序列检查：
            # 上游重发旧报文不应计为序列回退，也不应重复补发。
            if self._buffer.is_duplicate(point):
                logger.info("duplicate device=%s metric=%s seq=%s (idempotent accept)",
                            point.device_id, point.metric, point.seq)
                self._audit.record_decision(envelope, point, Decision.DUPLICATE,
                                            None, eff.version, eff.config)
                return IngestResult(accepted=True, point=point, duplicate=True)

            # 先入缓存再做会话/配额判定，保证补发 offset 等于接收顺序；
            # 判定拒绝则回滚（offset 空洞不影响后续记录的顺序）。
            stored = self._buffer.store(point)
            if stored is StoreResult.FULL:
                rej = Rejection(reason=RejectReason.BUFFER_FULL, envelope=envelope,
                                detail=f"buffer capacity {self._buffer.capacity} exhausted")
                logger.info("reject %s: %s", rej.reason.value, rej.detail)
                self._audit.record_decision(envelope, point, Decision.REJECTED,
                                            rej.reason, eff.version, eff.config)
                return IngestResult(accepted=False, rejection=rej)

            reason = self._sessions.check(point, policy)
            if reason is not None:
                self._buffer.rollback_last(point)
                rej = Rejection(reason=reason, envelope=envelope,
                                detail=f"device={point.device_id} seq={point.seq}")
                logger.info("reject %s: %s", rej.reason.value, rej.detail)
                self._audit.record_decision(envelope, point, Decision.REJECTED,
                                            reason, eff.version, eff.config)
                return IngestResult(accepted=False, rejection=rej)

            logger.info("stored device=%s metric=%s seq=%s (config v%d)",
                        point.device_id, point.metric, point.seq, eff.version)
            self._audit.record_decision(envelope, point, Decision.ACCEPTED,
                                        None, eff.version, eff.config)
            self.flush()
            return IngestResult(accepted=True, point=point)

    def flush(self) -> int:
        """下游可用时按序补发缓存数据，返回补发条数。

        补发只按 offset 顺序转发已受理数据，不重新做配额判定——受理时的
        判定结论（及其钉住的配置版本）不因后续配置变更而改变。
        """
        with self._lock:
            sent = 0
            for rec in self._buffer.pending():
                if self._downstream.send(rec):
                    self._buffer.ack_through(rec.offset)
                    sent += 1
                    logger.info("forwarded offset=%d device=%s seq=%d",
                                rec.offset, rec.point.device_id, rec.point.seq)
                else:
                    logger.info("downstream unavailable, %d record(s) remain buffered",
                                len(self._buffer))
                    break
            return sent
