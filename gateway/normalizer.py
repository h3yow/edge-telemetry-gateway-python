"""协议归一：把不同来源的原始报文归一到 TelemetryPoint（骨架）。"""
from __future__ import annotations

from typing import Protocol

from .models import RawEnvelope, Rejection, TelemetryPoint


class SourceAdapter(Protocol):
    """来源适配器协议：把原始报文映射为内部模型。"""

    source: str

    def normalize(self, envelope: RawEnvelope) -> TelemetryPoint | Rejection:
        ...


class Normalizer:
    """按来源分派适配器，未知来源给出可区分拒绝。"""

    def __init__(self) -> None:
        self._adapters: dict[str, SourceAdapter] = {}

    def register(self, adapter: SourceAdapter) -> None:
        raise NotImplementedError

    def normalize(self, envelope: RawEnvelope) -> TelemetryPoint | Rejection:
        raise NotImplementedError


class HttpJsonAdapter:
    """HTTP JSON 来源：{device_id, metric, value, ts_ms, seq}。"""

    source = "http-json"

    def normalize(self, envelope: RawEnvelope) -> TelemetryPoint | Rejection:
        raise NotImplementedError


class ModbusAdapter:
    """Modbus 轮询来源：{unit, register, raw, poll_ts_ms, poll_seq}。"""

    source = "modbus"

    def normalize(self, envelope: RawEnvelope) -> TelemetryPoint | Rejection:
        raise NotImplementedError
