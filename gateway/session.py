"""设备会话与接入配额：并发连接、上报频率、序列顺序。

判定规则（全部确定性、可复现）：
- 每台设备的并发会话数不得超过 max_connections_per_device；
- 上报必须携带该设备的活跃会话，否则视为会话状态冲突；
- 同一设备 seq 必须严格递增，回退（含同号重发）拒绝为 SEQUENCE_REGRESSION；
- 滑动窗口内上报次数超过 max_reports_per_window 拒绝为 QUOTA_EXCEEDED；
- 配额判定只影响超限设备本身，不影响其他设备；
- 配置以 version 递增，记录归一时打上版本号，历史语义可复现。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from .models import IngestResult, RejectReason, TelemetryRecord


@dataclass(frozen=True)
class QuotaConfig:
    """配额配置；version 递增，历史数据记录所用版本以复现语义。"""

    version: int = 1
    max_connections_per_device: int = 1
    max_reports_per_window: int = 10
    window_ms: int = 60_000


@dataclass
class _DeviceState:
    sessions: set[str] = field(default_factory=set)
    last_seq: int = 0
    report_times: deque[int] = field(default_factory=deque)


class SessionManager:
    """按设备维护会话状态并按配额判定上报是否可受理。"""

    def __init__(self, config: QuotaConfig) -> None:
        self._config = config
        self._devices: dict[str, _DeviceState] = {}

    @property
    def config(self) -> QuotaConfig:
        return self._config

    def update_config(self, config: QuotaConfig) -> None:
        """变更配额；要求版本严格递增，已受理记录不受影响。"""
        if config.version <= self._config.version:
            raise ValueError(
                f"配置版本必须递增: 当前 {self._config.version}, 收到 {config.version}")
        self._config = config

    def _state(self, device_id: str) -> _DeviceState:
        return self._devices.setdefault(device_id, _DeviceState())

    def open_session(self, device_id: str, session_id: str) -> IngestResult:
        st = self._state(device_id)
        if session_id in st.sessions:
            return IngestResult.reject(
                RejectReason.SESSION_CONFLICT,
                f"会话 {session_id} 已存在")
        if len(st.sessions) >= self._config.max_connections_per_device:
            return IngestResult.reject(
                RejectReason.SESSION_CONFLICT,
                f"设备 {device_id} 并发连接数达到上限 "
                f"{self._config.max_connections_per_device}")
        st.sessions.add(session_id)
        return IngestResult(accepted=True)

    def close_session(self, device_id: str, session_id: str) -> None:
        st = self._devices.get(device_id)
        if st:
            st.sessions.discard(session_id)

    def check(self, record: TelemetryRecord, session_id: str,
              now_ms: int) -> IngestResult:
        """按会话状态、序列顺序、频率配额依次判定（只校验，不提交）。

        判定通过后须由调用方在投递/暂存成功后调用 commit，
        保证被拒绝的报文不消耗序列号与配额。
        """
        st = self._state(record.device_id)

        if session_id not in st.sessions:
            return IngestResult.reject(
                RejectReason.SESSION_CONFLICT,
                f"会话 {session_id} 不是设备 {record.device_id} 的活跃会话",
                raw=record.raw, source_protocol=record.source_protocol)

        if record.seq <= st.last_seq:
            return IngestResult.reject(
                RejectReason.SEQUENCE_REGRESSION,
                f"seq {record.seq} 未严格递增（已受理到 {st.last_seq}）",
                raw=record.raw, source_protocol=record.source_protocol)

        cfg = self._config
        while st.report_times and st.report_times[0] <= now_ms - cfg.window_ms:
            st.report_times.popleft()
        if len(st.report_times) >= cfg.max_reports_per_window:
            return IngestResult.reject(
                RejectReason.QUOTA_EXCEEDED,
                f"窗口 {cfg.window_ms}ms 内已受理 {len(st.report_times)} 条，"
                f"达到配额 {cfg.max_reports_per_window}",
                raw=record.raw, source_protocol=record.source_protocol)

        return IngestResult.ok(record)

    def commit(self, record: TelemetryRecord, now_ms: int) -> None:
        """提交一次已受理上报：推进序列号并计入频率窗口。"""
        st = self._state(record.device_id)
        st.last_seq = record.seq
        st.report_times.append(now_ms)
