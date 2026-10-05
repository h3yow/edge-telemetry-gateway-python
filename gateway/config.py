"""按设备接入配置治理：差异化配置、热更新、版本历史。

确定性规则：
- 每台设备可覆盖默认配置；未覆盖的设备回落到默认配置；
- 更新按批提交：整批校验（字段合法 + 版本单调前进），任一不合法整批拒绝，
  已生效配置保持不变（先校验后生效，不在中途落盘）；
- apply_updates 与判定路径共用同一把锁（见 Gateway），单条报文的判定
  要么完全用旧版本、要么完全用新版本，不会混用；
- 每个设备保留有界版本历史（max_versions_per_device），供历史判定重放时
  按版本取回当时配置；历史满时淘汰最旧版本，内存有界。
"""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass

DEFAULT_CONFIG_KEY = ""  # 默认配置在内部表中的键（DeviceConfig.device_id=None）


@dataclass(frozen=True)
class DeviceConfig:
    """单设备接入配置（device_id=None 表示默认配置，未覆盖设备的回落）。"""

    device_id: str | None
    version: int               # 该设备配置版本，单调递增
    max_concurrent_sessions: int
    max_reports_per_window: int
    window_ms: int
    applied_at_ms: int = 0     # 生效时刻（审计用）

    def validate(self) -> tuple[str, ...]:
        """字段合法性校验，返回错误列表（空 = 合法）。"""
        errs: list[str] = []
        if self.version < 1:
            errs.append(f"version must be >= 1, got {self.version}")
        if self.max_concurrent_sessions < 1:
            errs.append(f"max_concurrent_sessions must be >= 1, got {self.max_concurrent_sessions}")
        if self.max_reports_per_window < 1:
            errs.append(f"max_reports_per_window must be >= 1, got {self.max_reports_per_window}")
        if self.window_ms < 1:
            errs.append(f"window_ms must be >= 1, got {self.window_ms}")
        if self.device_id is not None and not self.device_id:
            errs.append("device_id must be None (default) or non-empty str")
        return tuple(errs)


@dataclass(frozen=True)
class ConfigUpdateResult:
    """一批配置更新的结果：整批接受或整批拒绝。"""

    applied: bool
    errors: tuple[str, ...] = ()
    effective: tuple[DeviceConfig, ...] = ()


def _key(device_id: str | None) -> str:
    return device_id if device_id else DEFAULT_CONFIG_KEY


class ConfigRegistry:
    """按设备配置注册表：热更新、有界版本历史、并发安全。"""

    def __init__(self, default: DeviceConfig, max_versions_per_device: int = 16) -> None:
        if default.device_id is not None:
            raise ValueError("default config must have device_id=None")
        if (errs := default.validate()):
            raise ValueError(f"invalid default config: {errs}")
        if max_versions_per_device < 1:
            raise ValueError("max_versions_per_device must be >= 1")
        self._lock = threading.RLock()
        self._max_versions = max_versions_per_device
        self._current: dict[str, DeviceConfig] = {DEFAULT_CONFIG_KEY: default}
        self._history: dict[str, deque[DeviceConfig]] = {}

    @property
    def lock(self) -> threading.RLock:
        """与判定路径共用的互斥锁：配置切换与上报/补发不会交错出混合语义。"""
        return self._lock

    def effective_for(self, device_id: str) -> DeviceConfig:
        """设备当前生效配置（有覆盖用覆盖，否则回落默认）。"""
        with self._lock:
            return self._current.get(_key(device_id), self._current[DEFAULT_CONFIG_KEY])

    def version_of(self, device_id: str) -> int:
        return self.effective_for(device_id).version

    def apply_updates(self, updates: list[DeviceConfig]) -> ConfigUpdateResult:
        """整批校验并生效；任一非法则整批拒绝，原配置保持不变。"""
        with self._lock:
            errors: list[str] = []
            # 在副本上预演整批：批内同设备多条按顺序也必须版本递增
            staged: dict[str, DeviceConfig] = dict(self._current)
            for u in updates:
                who = u.device_id if u.device_id is not None else "<default>"
                for e in u.validate():
                    errors.append(f"{who}: {e}")
                k = _key(u.device_id)
                # 版本单调性只在同一条配置链上检查：设备首次覆盖自成一链，
                # 与默认配置的版本无关；批内同设备多条按顺序也必须递增。
                if k in staged and u.version <= staged[k].version:
                    errors.append(
                        f"{who}: version must increase monotonically "
                        f"(current {staged[k].version}, got {u.version})")
                staged[k] = u
            if errors:
                return ConfigUpdateResult(applied=False, errors=tuple(errors))
            # 全部合法才生效：旧版本推入有界历史
            for u in updates:
                k = _key(u.device_id)
                old = self._current.get(k)
                if old is not None and k in self._current:
                    hist = self._history.setdefault(k, deque())
                    hist.append(old)
                    while len(hist) > self._max_versions:
                        hist.popleft()
                self._current[k] = u
            return ConfigUpdateResult(
                applied=True,
                effective=tuple(self._current[_key(u.device_id)] for u in updates),
            )

    def config_at_version(self, device_id: str, version: int) -> DeviceConfig | None:
        """按版本取回配置（含当前生效版本）；版本已淘汰或从未存在返回 None。

        先查设备自己的配置链；设备覆盖生效之前的判定回落到默认链的版本
        （设备首次覆盖前的历史记录记的是默认链版本）。
        """
        with self._lock:
            for chain in (_key(device_id), DEFAULT_CONFIG_KEY):
                cur = self._current.get(chain)
                if cur is not None and cur.version == version:
                    return cur
                for cfg in self._history.get(chain, ()):
                    if cfg.version == version:
                        return cfg
            return None
