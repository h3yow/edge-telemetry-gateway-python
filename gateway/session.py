"""设备会话与接入配额管理。

确定性规则：
- 上报前必须先建立连接（open_connection），否则 SESSION_CONFLICT；
- 并发连接数超过该设备生效配置的配额 -> QUOTA_EXCEEDED，且不影响其他设备；
- 同一设备 seq 必须严格递增，回退或重复 -> SEQUENCE_REGRESSION；
- 滑窗（window_ms）内上报数超过该设备生效配置的配额 -> QUOTA_EXCEEDED；
- 配置按设备生效（ConfigRegistry）：判定取该设备当时生效的配置版本；
  版本切换后该设备的频率窗口在新版本下重新起算，序列连续性（last_seq，
  设备固有状态）保留；其他设备的会话与窗口完全不受影响；
- 每次判定把生效配置版本记入会话状态（policy_version），历史语义可复现。
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .config import ConfigRegistry, DeviceConfig
from .models import RejectReason, TelemetryPoint


@dataclass(frozen=True)
class QuotaPolicy:
    """配额策略（兼容视图）：配置治理引入前的旧构造方式，内部转为默认 DeviceConfig。"""

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


def _to_default_config(policy: QuotaPolicy) -> DeviceConfig:
    return DeviceConfig(
        device_id=None,
        version=policy.version,
        max_concurrent_sessions=policy.max_concurrent_sessions_per_device,
        max_reports_per_window=policy.max_reports_per_window,
        window_ms=policy.window_ms,
    )


class SessionManager:
    """管理设备会话、序列连续性检查与按设备生效配置的配额判定。"""

    def __init__(self, policy: QuotaPolicy | None = None,
                 registry: ConfigRegistry | None = None,
                 config_provider: Callable[[str], DeviceConfig] | None = None) -> None:
        if config_provider is None and registry is None:
            if policy is None:
                raise ValueError("either policy, registry or config_provider must be given")
            registry = ConfigRegistry(_to_default_config(policy))
        self._registry = registry
        self._provider = config_provider
        self._sessions: dict[str, DeviceSession] = {}

    def _config_for(self, device_id: str) -> DeviceConfig:
        """判定取配置的唯一入口：默认走注册表，重放时走冻结视角 provider。"""
        if self._provider is not None:
            return self._provider(device_id)
        assert self._registry is not None
        return self._registry.effective_for(device_id)

    def config_version_of(self, device_id: str) -> int:
        """该设备当前（判定视角下）生效的配置版本。"""
        return self._config_for(device_id).version

    @property
    def registry(self) -> ConfigRegistry | None:
        return self._registry

    @property
    def policy(self) -> QuotaPolicy:
        """默认配置的 QuotaPolicy 兼容视图。"""
        d = self._registry.effective_for("")
        return QuotaPolicy(version=d.version,
                           max_concurrent_sessions_per_device=d.max_concurrent_sessions,
                           max_reports_per_window=d.max_reports_per_window,
                           window_ms=d.window_ms)

    def session_of(self, device_id: str) -> DeviceSession | None:
        return self._sessions.get(device_id)

    def update_policy(self, policy: QuotaPolicy) -> None:
        """更新默认配置（兼容入口）；版本不前进时整批拒绝并抛错。"""
        result = self._registry.apply_updates([_to_default_config(policy)])
        if not result.applied:
            raise ValueError("; ".join(result.errors))

    def open_connection(self, device_id: str) -> RejectReason | None:
        """建立连接；超出该设备生效配置的并发配额返回 QUOTA_EXCEEDED。"""
        cfg = self._config_for(device_id)
        s = self._sessions.get(device_id)
        if s is None:
            s = DeviceSession(
                device_id=device_id,
                active_connections=0,
                last_seq=-1,
                window_start_ms=0,
                window_count=0,
                policy_version=cfg.version,
            )
            self._sessions[device_id] = s
        if s.active_connections >= cfg.max_concurrent_sessions:
            return RejectReason.QUOTA_EXCEEDED
        s.active_connections += 1
        return None

    def close_connection(self, device_id: str) -> None:
        s = self._sessions.get(device_id)
        if s is not None and s.active_connections > 0:
            s.active_connections -= 1

    def check(self, point: TelemetryPoint) -> RejectReason | None:
        """校验会话状态、序列连续性与频率配额；通过则推进会话状态，返回 None。

        判定使用调用时刻该设备生效的配置；若生效版本与会话上次判定版本不同，
        该设备（且仅该设备）的频率窗口在新版本下重新起算。
        """
        cfg = self._config_for(point.device_id)
        s = self._sessions.get(point.device_id)
        if s is None or s.active_connections <= 0:
            return RejectReason.SESSION_CONFLICT  # 无活跃会话的上报
        if point.seq <= s.last_seq:
            return RejectReason.SEQUENCE_REGRESSION
        if cfg.version != s.policy_version:
            # 该设备配置已切换：窗口在新版本下重新起算；last_seq 保留。
            s.window_count = 0
            s.window_start_ms = 0
            s.policy_version = cfg.version
        # 滑窗频率检查（按网关接收时刻）
        if s.window_count == 0 or point.received_at_ms - s.window_start_ms >= cfg.window_ms:
            s.window_start_ms = point.received_at_ms
            s.window_count = 0
        if s.window_count >= cfg.max_reports_per_window:
            return RejectReason.QUOTA_EXCEEDED
        s.window_count += 1
        s.last_seq = point.seq
        return None
