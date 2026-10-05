"""按设备接入配置治理：差异化配额、版本化热更新、原子批量应用。

确定性规则：
- 每台设备可有独立覆盖配置；未覆盖的设备使用默认配置；
- 配置按版本单调递增热更新，无需重启，应用后立即对目标设备生效，
  只影响被覆盖的设备，其他设备照常按各自生效配置接入；
- 批量更新整体校验：任一配置非法或版本未前进 -> 整批拒绝（ConfigError），
  原生效配置保持不变（先校验后替换，替换本身只是字典整体换引用，无中间态）；
- 注册表保留有界的历史版本快照（默认 32 版），供判定追溯与重放使用；
  超出保留范围的最旧版本被淘汰，内存不无限增长。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .session import QuotaPolicy

DEFAULT_HISTORY_LIMIT = 32  # 保留的历史配置版本数上限（有界）


class ConfigError(ValueError):
    """配置非法或版本未前进：整批拒绝，原生效配置不变。"""


def _validate_positive(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{name} must be a positive int, got {value!r}")
    return value


@dataclass(frozen=True)
class DeviceConfig:
    """单设备接入配置：并发连接上限 + 滑窗频率上限。"""

    max_concurrent_sessions_per_device: int
    max_reports_per_window: int
    window_ms: int

    def validate(self) -> None:
        """非法配置抛 ConfigError（由调用方整批拒绝）。"""
        _validate_positive("max_concurrent_sessions_per_device",
                           self.max_concurrent_sessions_per_device)
        _validate_positive("max_reports_per_window", self.max_reports_per_window)
        _validate_positive("window_ms", self.window_ms)

    def as_policy(self, version: int) -> QuotaPolicy:
        """带上生效版本号，转成会话层可执行的配额策略。"""
        return QuotaPolicy(
            version=version,
            max_concurrent_sessions_per_device=self.max_concurrent_sessions_per_device,
            max_reports_per_window=self.max_reports_per_window,
            window_ms=self.window_ms,
        )


@dataclass(frozen=True)
class EffectiveConfig:
    """某设备某时刻生效的配置及其版本与来源。"""

    config: DeviceConfig
    version: int
    scope: str  # "device" | "default"


@dataclass(frozen=True)
class ConfigUpdate:
    """一批配置变更：可含默认配置与多设备覆盖；version 必须单调前进。

    devices 中值为 None 表示移除该设备的覆盖（回落到默认配置）。
    """

    version: int
    default: DeviceConfig | None = None
    devices: dict[str, DeviceConfig | None] = field(default_factory=dict)


@dataclass(frozen=True)
class ConfigSnapshot:
    """某一版本的完整配置快照（默认 + 全部设备覆盖），供追溯/重放。"""

    version: int
    default: DeviceConfig
    devices: dict[str, DeviceConfig]

    def effective_for(self, device_id: str) -> EffectiveConfig:
        if device_id in self.devices:
            return EffectiveConfig(self.devices[device_id], self.version, "device")
        return EffectiveConfig(self.default, self.version, "default")


class ConfigRegistry:
    """版本化配置注册表：原子批量应用、按设备取生效配置、有界历史。"""

    def __init__(self, default: DeviceConfig, version: int = 1,
                 history_limit: int = DEFAULT_HISTORY_LIMIT) -> None:
        if history_limit <= 0:
            raise ValueError("history_limit must be positive")
        default.validate()
        self._lock = threading.RLock()
        self._default = default
        self._devices: dict[str, DeviceConfig] = {}
        self._version = version
        self._history_limit = history_limit
        self._history: dict[int, ConfigSnapshot] = {}
        self._remember()

    # -- 内部 -------------------------------------------------------------

    def _remember(self) -> None:
        """把当前状态存为有界历史快照；超出上限淘汰最旧版本。"""
        self._history[self._version] = ConfigSnapshot(
            version=self._version, default=self._default, devices=dict(self._devices))
        while len(self._history) > self._history_limit:
            oldest = min(self._history)
            del self._history[oldest]

    # -- 查询 -------------------------------------------------------------

    @property
    def version(self) -> int:
        return self._version

    def effective_for(self, device_id: str) -> EffectiveConfig:
        """目标设备当前生效的配置（覆盖优先，否则默认）。"""
        with self._lock:
            if device_id in self._devices:
                return EffectiveConfig(self._devices[device_id], self._version, "device")
            return EffectiveConfig(self._default, self._version, "default")

    def snapshot_for_version(self, version: int) -> ConfigSnapshot | None:
        """查历史版本快照（有界，超出保留范围返回 None）。"""
        with self._lock:
            return self._history.get(version)

    # -- 变更 -------------------------------------------------------------

    def apply(self, update: ConfigUpdate) -> None:
        """原子应用一批变更；任一非法或版本未前进 -> ConfigError，状态不变。"""
        with self._lock:
            # 1) 整体校验：任何一项不合法都直接抛错，此时状态尚未被触碰。
            if update.version <= self._version:
                raise ConfigError(
                    f"config version must increase: {update.version} <= {self._version}")
            new_default = update.default if update.default is not None else self._default
            new_default.validate()
            new_devices = dict(self._devices)
            for device_id, cfg in update.devices.items():
                if not device_id:
                    raise ConfigError("device_id must be non-empty")
                if cfg is None:
                    new_devices.pop(device_id, None)  # 移除覆盖，回落默认
                else:
                    cfg.validate()
                    new_devices[device_id] = cfg
            # 2) 全部合法后一次性替换，无中间态；并发读只会看到旧版或新版整体。
            self._default = new_default
            self._devices = new_devices
            self._version = update.version
            self._remember()
