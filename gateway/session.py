"""设备会话与接入配额管理（骨架）。"""
from __future__ import annotations

from dataclasses import dataclass

from .models import RejectReason, TelemetryPoint


@dataclass(frozen=True)
class QuotaPolicy:
    """配额策略：配置变更带版本号，历史判定可复现。"""

    version: int
    max_concurrent_sessions_per_device: int
    max_reports_per_window: int     # 滑窗内最大上报条数
    window_ms: int                  # 滑窗长度


@dataclass
class DeviceSession:
    """单设备会话状态。"""

    device_id: str
    active_connections: int
    last_seq: int
    window_start_ms: int
    window_count: int
    policy_version: int


class SessionManager:
    """管理设备会话、序列连续性检查与配额判定。"""

    def __init__(self, policy: QuotaPolicy) -> None:
        self._policy = policy
        self._sessions: dict[str, DeviceSession] = {}

    @property
    def policy(self) -> QuotaPolicy:
        return self._policy

    def update_policy(self, policy: QuotaPolicy) -> None:
        raise NotImplementedError

    def open_connection(self, device_id: str) -> RejectReason | None:
        """建立连接；超出并发配额返回拒绝原因。"""
        raise NotImplementedError

    def close_connection(self, device_id: str) -> None:
        raise NotImplementedError

    def check(self, point: TelemetryPoint) -> RejectReason | None:
        """校验序列回退与频率配额；通过则推进会话状态，返回 None。"""
        raise NotImplementedError
