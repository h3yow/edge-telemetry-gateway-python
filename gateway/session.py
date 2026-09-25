"""设备会话与接入配额管理。

确定性规则：
- 上报前必须先建立连接（open_connection），否则 SESSION_CONFLICT；
- 并发连接数超过配额 -> QUOTA_EXCEEDED，且不影响其他设备；
- 同一设备 seq 必须严格递增，回退或重复 -> SEQUENCE_REGRESSION；
- 滑窗（window_ms）内上报数超过配额 -> QUOTA_EXCEEDED；
- 配置变更（update_policy）递增版本号：序列连续性（last_seq）是设备固有状态，
  予以保留；频率窗口计数在新版本下重新起算。每次判定记录当时的 policy_version，
  历史语义可复现。
"""
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

    def session_of(self, device_id: str) -> DeviceSession | None:
        return self._sessions.get(device_id)

    def update_policy(self, policy: QuotaPolicy) -> None:
        if policy.version <= self._policy.version:
            raise ValueError("policy version must increase monotonically")
        self._policy = policy
        # 频率窗口在新版本下重新起算；last_seq 保留以维持序列语义。
        for s in self._sessions.values():
            s.window_count = 0
            s.window_start_ms = 0
            s.policy_version = policy.version

    def open_connection(self, device_id: str) -> RejectReason | None:
        """建立连接；超出并发配额返回 QUOTA_EXCEEDED。"""
        s = self._sessions.get(device_id)
        if s is None:
            s = DeviceSession(
                device_id=device_id,
                active_connections=0,
                last_seq=-1,
                window_start_ms=0,
                window_count=0,
                policy_version=self._policy.version,
            )
            self._sessions[device_id] = s
        if s.active_connections >= self._policy.max_concurrent_sessions_per_device:
            return RejectReason.QUOTA_EXCEEDED
        s.active_connections += 1
        return None

    def close_connection(self, device_id: str) -> None:
        s = self._sessions.get(device_id)
        if s is not None and s.active_connections > 0:
            s.active_connections -= 1

    def check(self, point: TelemetryPoint) -> RejectReason | None:
        """校验会话状态、序列连续性与频率配额；通过则推进会话状态，返回 None。"""
        s = self._sessions.get(point.device_id)
        if s is None or s.active_connections <= 0:
            return RejectReason.SESSION_CONFLICT  # 无活跃会话的上报
        if point.seq <= s.last_seq:
            return RejectReason.SEQUENCE_REGRESSION
        # 滑窗频率检查（按网关接收时刻）
        if s.window_count == 0 or point.received_at_ms - s.window_start_ms >= self._policy.window_ms:
            s.window_start_ms = point.received_at_ms
            s.window_count = 0
        if s.window_count >= self._policy.max_reports_per_window:
            return RejectReason.QUOTA_EXCEEDED
        s.window_count += 1
        s.last_seq = point.seq
        s.policy_version = self._policy.version
        return None
