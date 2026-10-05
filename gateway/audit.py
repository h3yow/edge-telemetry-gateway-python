"""判定追溯：有界判定日志、按设备/时间查询、确定性重放。

确定性规则：
- 每条上报的判定（accepted / duplicate / rejected+reason）连同当时生效的
  配置版本与配置快照一起落日志；连接/断开事件同样记录，保证重放时可还原会话状态；
- 日志容量有界（超出淘汰最旧记录并计数），内存不无限增长；
- 支持按设备、按时间段查询；
- 重放：用每条记录里钉住的配置快照，在全新会话/缓存状态上原样重跑判定流水线，
  结果必须与记录完全一致（accepted/duplicate/rejected+reason 逐项比对）。
  已收下但未转发的数据，其判定记录钉的是受理时刻的配置，之后改配置不影响重放结论。
"""
from __future__ import annotations

import enum
import logging
import threading
from collections import deque
from dataclasses import dataclass

from .buffer import SpoolBuffer, StoreResult
from .config import DeviceConfig
from .models import RawEnvelope, Rejection, RejectReason, TelemetryPoint
from .normalizer import HttpJsonAdapter, ModbusAdapter, Normalizer
from .session import SessionManager

logger = logging.getLogger("gateway.audit")

DEFAULT_AUDIT_CAPACITY = 10_000  # 判定日志容量上限（有界）


class EntryKind(enum.Enum):
    DECISION = "decision"      # 一条上报的判定
    CONNECT = "connect"        # 设备建连（含并发配额判定结果）
    DISCONNECT = "disconnect"  # 设备断连


class Decision(enum.Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


@dataclass(frozen=True)
class AuditEntry:
    """一条追溯记录：输入 + 判定 + 当时生效的配置（版本与快照）。"""

    seq_no: int                     # 日志内单调序号（全局顺序）
    kind: EntryKind
    device_id: str
    received_at_ms: int             # 事件发生时刻（网关时钟）
    config_version: int             # 判定时生效的配置版本
    config: DeviceConfig            # 判定时生效的配置快照（重放钉版用）
    envelope: RawEnvelope | None    # 原始报文（connect/disconnect 为 None）
    point: TelemetryPoint | None    # 归一化结果（归一失败为 None）
    decision: Decision | None       # 上报判定结果（事件类记录为 None）
    reason: RejectReason | None     # 拒绝原因（未拒绝为 None；建连超限也记录）


@dataclass(frozen=True)
class ReplayMismatch:
    """重放结果与历史记录不一致的明细。"""

    entry: AuditEntry
    actual_decision: Decision | None
    actual_reason: RejectReason | None


def _device_of(envelope: RawEnvelope, point: TelemetryPoint | None) -> str:
    if point is not None:
        return point.device_id
    dev = envelope.payload.get("device_id")
    return dev if isinstance(dev, str) else ""


class AuditLog:
    """有界判定日志：追加、查询、淘汰计数，线程安全。"""

    def __init__(self, capacity: int = DEFAULT_AUDIT_CAPACITY) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._entries: deque[AuditEntry] = deque(maxlen=capacity)
        self._dropped = 0
        self._seq = 0
        self._lock = threading.Lock()

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def dropped(self) -> int:
        """因容量上限被淘汰的记录条数。"""
        return self._dropped

    def __len__(self) -> int:
        return len(self._entries)

    # -- 记录 -------------------------------------------------------------

    def _append(self, entry: AuditEntry) -> AuditEntry:
        if len(self._entries) == self._capacity:
            self._dropped += 1
        self._entries.append(entry)
        return entry

    def record_decision(self, envelope: RawEnvelope, point: TelemetryPoint | None,
                        decision: Decision, reason: RejectReason | None,
                        config_version: int, config: DeviceConfig) -> AuditEntry:
        """记录一条上报判定。"""
        with self._lock:
            self._seq += 1
            entry = AuditEntry(
                seq_no=self._seq, kind=EntryKind.DECISION,
                device_id=_device_of(envelope, point),
                received_at_ms=envelope.received_at_ms,
                config_version=config_version, config=config,
                envelope=envelope, point=point, decision=decision, reason=reason,
            )
            logger.info("audit #%d decision=%s device=%s reason=%s config_v=%d",
                        entry.seq_no, decision.value, entry.device_id,
                        reason.value if reason else None, config_version)
            return self._append(entry)

    def record_connect(self, device_id: str, reason: RejectReason | None,
                       config_version: int, config: DeviceConfig,
                       at_ms: int = 0) -> AuditEntry:
        """记录一次建连事件（reason 非空表示并发配额拒绝）。"""
        with self._lock:
            self._seq += 1
            entry = AuditEntry(
                seq_no=self._seq, kind=EntryKind.CONNECT, device_id=device_id,
                received_at_ms=at_ms, config_version=config_version, config=config,
                envelope=None, point=None, decision=None, reason=reason,
            )
            logger.info("audit #%d connect device=%s reason=%s config_v=%d",
                        entry.seq_no, device_id,
                        reason.value if reason else None, config_version)
            return self._append(entry)

    def record_disconnect(self, device_id: str, config_version: int,
                          config: DeviceConfig, at_ms: int = 0) -> AuditEntry:
        """记录一次断连事件。"""
        with self._lock:
            self._seq += 1
            entry = AuditEntry(
                seq_no=self._seq, kind=EntryKind.DISCONNECT, device_id=device_id,
                received_at_ms=at_ms, config_version=config_version, config=config,
                envelope=None, point=None, decision=None, reason=None,
            )
            return self._append(entry)

    # -- 查询 -------------------------------------------------------------

    def query(self, device_id: str | None = None, start_ms: int | None = None,
              end_ms: int | None = None) -> list[AuditEntry]:
        """按设备/时间段过滤，按 seq_no 升序返回。"""
        with self._lock:
            return [e for e in self._entries
                    if (device_id is None or e.device_id == device_id)
                    and (start_ms is None or e.received_at_ms >= start_ms)
                    and (end_ms is None or e.received_at_ms <= end_ms)]

    def entries(self) -> list[AuditEntry]:
        """全部现存记录（seq_no 升序）。"""
        with self._lock:
            return list(self._entries)


def _default_normalizer() -> Normalizer:
    n = Normalizer()
    n.register(HttpJsonAdapter())
    n.register(ModbusAdapter())
    return n


def replay(entries: list[AuditEntry],
           normalizer: Normalizer | None = None) -> list[ReplayMismatch]:
    """用记录里钉住的配置快照原样重放判定流水线，返回不一致明细（空 = 完全复现）。

    重放状态全新构造：归一规则与网关同源；缓存容量取判定条数+1（重放不确认
    截断，保证容量本身不会成为差异来源）；每条记录使用其钉住的 config/version，
    与当前生效配置无关。
    """
    norm = normalizer if normalizer is not None else _default_normalizer()
    decisions = [e for e in entries if e.kind is EntryKind.DECISION]
    buffer = SpoolBuffer(capacity=max(1, len(decisions) + 1))
    sessions = SessionManager(_default_normalizer_policy_anchor())
    mismatches: list[ReplayMismatch] = []

    for e in sorted(entries, key=lambda x: x.seq_no):
        policy = e.config.as_policy(e.config_version)
        if e.kind is EntryKind.CONNECT:
            actual = sessions.open_connection(e.device_id, policy)
            if actual is not e.reason:
                mismatches.append(ReplayMismatch(e, None, actual))
            continue
        if e.kind is EntryKind.DISCONNECT:
            sessions.close_connection(e.device_id)
            continue

        # DECISION：完整重跑 归一 -> 去重/缓存 -> 会话配额
        assert e.envelope is not None
        normalized = norm.normalize(e.envelope)
        if isinstance(normalized, Rejection):
            actual_decision, actual_reason = Decision.REJECTED, normalized.reason
        else:
            point = normalized
            if buffer.is_duplicate(point):
                actual_decision, actual_reason = Decision.DUPLICATE, None
            elif buffer.store(point) is StoreResult.FULL:
                actual_decision, actual_reason = Decision.REJECTED, RejectReason.BUFFER_FULL
            else:
                reason = sessions.check(point, policy)
                if reason is not None:
                    buffer.rollback_last(point)
                    actual_decision, actual_reason = Decision.REJECTED, reason
                else:
                    actual_decision, actual_reason = Decision.ACCEPTED, None
        if actual_decision is not e.decision or actual_reason is not e.reason:
            mismatches.append(ReplayMismatch(e, actual_decision, actual_reason))
        logger.info("replay #%d device=%s expected=%s/%s actual=%s/%s",
                    e.seq_no, e.device_id, e.decision.value if e.decision else None,
                    e.reason.value if e.reason else None,
                    actual_decision.value, actual_reason.value if actual_reason else None)
    return mismatches


def _default_normalizer_policy_anchor():
    """会话管理器的占位默认策略（重放时每条记录都显式传入钉住的策略）。"""
    from .session import QuotaPolicy
    return QuotaPolicy(version=0, max_concurrent_sessions_per_device=1,
                       max_reports_per_window=1, window_ms=1)
