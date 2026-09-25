"""网关编排：归一 -> 会话/配额 -> 转发或暂存 -> 恢复补发。

转发确定规则：
- 下游可用且暂存为空：直接投递；
- 下游不可用或暂存非空：入队暂存，保证全局按序，不插队；
- 暂存满 / 重复入队：以 BUFFER_FULL / DUPLICATE 拒绝，不静默丢弃；
- 下游恢复：按入队顺序补发，失败即停，剩余保留。
"""

from __future__ import annotations

from .buffer import ReplayBuffer
from .downstream import SimulatedDownstream
from .models import ForwardResult, IngestResult, RejectReason
from .protocols import normalize
from .session import QuotaConfig, SessionManager


class Gateway:
    """对外统一入口：接入、暂存、补发、配置变更。"""

    def __init__(self, quota: QuotaConfig, buffer_capacity: int = 1000) -> None:
        self._sessions = SessionManager(quota)
        self._buffer = ReplayBuffer(buffer_capacity)
        self._downstream = SimulatedDownstream()

    @property
    def buffer(self) -> ReplayBuffer:
        return self._buffer

    @property
    def downstream(self) -> SimulatedDownstream:
        return self._downstream

    @property
    def quota(self) -> QuotaConfig:
        return self._sessions.config

    def open_session(self, device_id: str, session_id: str) -> IngestResult:
        return self._sessions.open_session(device_id, session_id)

    def close_session(self, device_id: str, session_id: str) -> None:
        self._sessions.close_session(device_id, session_id)

    def update_quota(self, quota: QuotaConfig) -> None:
        self._sessions.update_config(quota)

    def ingest(self, raw: str, protocol: str, device_id: str,
               session_id: str, now_ms: int) -> IngestResult:
        """接入一条原始报文，返回可区分原因的判定结果。"""
        result = normalize(raw, protocol, now_ms,
                           config_version=self._sessions.config.version)
        if not result.accepted:
            return result
        record = result.record
        assert record is not None

        if record.device_id != device_id:
            return IngestResult.reject(
                RejectReason.SESSION_CONFLICT,
                f"报文设备 {record.device_id} 与连接设备 {device_id} 不一致",
                raw=raw, source_protocol=protocol)

        result = self._sessions.check(record, session_id, now_ms)
        if not result.accepted:
            return result

        if self._downstream.available and len(self._buffer) == 0:
            if self._downstream.deliver(record):
                self._sessions.commit(record, now_ms)
                return IngestResult(accepted=True, record=record,
                                    detail="delivered")

        err = self._buffer.enqueue(record)
        if err is None:
            self._sessions.commit(record, now_ms)
        if err == "duplicate":
            return IngestResult.reject(
                RejectReason.DUPLICATE,
                f"设备 {record.device_id} seq {record.seq} 已在暂存队列中",
                raw=raw, source_protocol=protocol)
        if err == "buffer_full":
            return IngestResult.reject(
                RejectReason.BUFFER_FULL,
                f"暂存队列已满（容量 {self._buffer.capacity}）",
                raw=raw, source_protocol=protocol)
        return IngestResult(accepted=True, record=record, detail="buffered")

    def set_downstream_available(self, available: bool,
                                 now_ms: int) -> list[ForwardResult]:
        """切换下游可用性；恢复时按序补发暂存并返回补发流水。"""
        self._downstream.set_available(available)
        if not available:
            return []
        return self._buffer.replay(self._downstream.deliver, now_ms)
