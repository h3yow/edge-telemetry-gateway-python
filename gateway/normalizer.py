"""协议归一：把不同来源的原始报文归一到 TelemetryPoint。

规则：
- 必填字段缺失 -> RejectReason.MISSING_FIELD
- 时间戳不可解析或与接收时刻偏差超过容忍窗口 -> RejectReason.BAD_TIMESTAMP
- 数值/序列号类型非法 -> RejectReason.MISSING_FIELD（视为字段无效，detail 说明）
- 未注册来源 -> RejectReason.UNKNOWN_SOURCE
所有拒绝都携带原始 RawEnvelope，保证出处可追溯。
"""
from __future__ import annotations

from typing import Any, Protocol

from .models import RawEnvelope, Rejection, RejectReason, TelemetryPoint

DEFAULT_MAX_SKEW_MS = 24 * 3600 * 1000  # 设备时钟与网关时钟的最大容忍偏差（1 天）


class SourceAdapter(Protocol):
    """来源适配器协议：把原始报文映射为内部模型。"""

    source: str

    def normalize(self, envelope: RawEnvelope) -> TelemetryPoint | Rejection:
        ...


def _reject(envelope: RawEnvelope, reason: RejectReason, detail: str) -> Rejection:
    return Rejection(reason=reason, envelope=envelope, detail=detail)


def _require(envelope: RawEnvelope, fields: list[str]) -> Rejection | None:
    for f in fields:
        if f not in envelope.payload or envelope.payload[f] is None:
            return _reject(envelope, RejectReason.MISSING_FIELD, f"required field '{f}' missing")
    return None


def _check_ts(envelope: RawEnvelope, ts_ms: Any, max_skew_ms: int) -> Rejection | None:
    if isinstance(ts_ms, bool) or not isinstance(ts_ms, int):
        return _reject(envelope, RejectReason.BAD_TIMESTAMP, f"ts not an int ms: {ts_ms!r}")
    if ts_ms < 0:
        return _reject(envelope, RejectReason.BAD_TIMESTAMP, f"negative ts_ms: {ts_ms}")
    if abs(ts_ms - envelope.received_at_ms) > max_skew_ms:
        return _reject(
            envelope,
            RejectReason.BAD_TIMESTAMP,
            f"ts_ms={ts_ms} deviates from received_at_ms={envelope.received_at_ms} "
            f"beyond skew {max_skew_ms}ms",
        )
    return None


def _check_seq(envelope: RawEnvelope, seq: Any) -> Rejection | None:
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
        return _reject(envelope, RejectReason.MISSING_FIELD, f"invalid seq: {seq!r}")
    return None


def _check_value(envelope: RawEnvelope, value: Any) -> Rejection | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return _reject(envelope, RejectReason.MISSING_FIELD, f"invalid numeric value: {value!r}")
    return None


class Normalizer:
    """按来源分派适配器，未知来源给出可区分拒绝。"""

    def __init__(self) -> None:
        self._adapters: dict[str, SourceAdapter] = {}

    def register(self, adapter: SourceAdapter) -> None:
        self._adapters[adapter.source] = adapter

    def normalize(self, envelope: RawEnvelope) -> TelemetryPoint | Rejection:
        adapter = self._adapters.get(envelope.source)
        if adapter is None:
            return _reject(envelope, RejectReason.UNKNOWN_SOURCE,
                           f"no adapter for source '{envelope.source}'")
        return adapter.normalize(envelope)


class HttpJsonAdapter:
    """HTTP JSON 来源：{device_id, metric, value, ts_ms, seq}。"""

    source = "http-json"

    def __init__(self, max_skew_ms: int = DEFAULT_MAX_SKEW_MS) -> None:
        self._max_skew_ms = max_skew_ms

    def normalize(self, envelope: RawEnvelope) -> TelemetryPoint | Rejection:
        p = envelope.payload
        if (r := _require(envelope, ["device_id", "metric", "value", "ts_ms", "seq"])) is not None:
            return r
        if not isinstance(p["device_id"], str) or not p["device_id"]:
            return _reject(envelope, RejectReason.MISSING_FIELD, "device_id must be non-empty str")
        if not isinstance(p["metric"], str) or not p["metric"]:
            return _reject(envelope, RejectReason.MISSING_FIELD, "metric must be non-empty str")
        if (r := _check_value(envelope, p["value"])) is not None:
            return r
        if (r := _check_ts(envelope, p["ts_ms"], self._max_skew_ms)) is not None:
            return r
        if (r := _check_seq(envelope, p["seq"])) is not None:
            return r
        return TelemetryPoint(
            device_id=p["device_id"],
            metric=p["metric"],
            value=float(p["value"]),
            ts_ms=p["ts_ms"],
            seq=p["seq"],
            source=envelope.source,
            received_at_ms=envelope.received_at_ms,
        )


class ModbusAdapter:
    """Modbus 轮询来源：{unit, register, raw, poll_ts_ms, poll_seq}。

    归一映射：device_id=f"modbus-{unit}"，metric=f"register.{register}"，
    value=float(raw)，ts_ms=poll_ts_ms，seq=poll_seq。
    """

    source = "modbus"

    def __init__(self, max_skew_ms: int = DEFAULT_MAX_SKEW_MS) -> None:
        self._max_skew_ms = max_skew_ms

    def normalize(self, envelope: RawEnvelope) -> TelemetryPoint | Rejection:
        p = envelope.payload
        if (r := _require(envelope, ["unit", "register", "raw", "poll_ts_ms", "poll_seq"])) is not None:
            return r
        if isinstance(p["unit"], bool) or not isinstance(p["unit"], int) or p["unit"] < 0:
            return _reject(envelope, RejectReason.MISSING_FIELD, f"invalid unit: {p['unit']!r}")
        if isinstance(p["register"], bool) or not isinstance(p["register"], int) or p["register"] < 0:
            return _reject(envelope, RejectReason.MISSING_FIELD, f"invalid register: {p['register']!r}")
        if (r := _check_value(envelope, p["raw"])) is not None:
            return r
        if (r := _check_ts(envelope, p["poll_ts_ms"], self._max_skew_ms)) is not None:
            return r
        if (r := _check_seq(envelope, p["poll_seq"])) is not None:
            return r
        return TelemetryPoint(
            device_id=f"modbus-{p['unit']}",
            metric=f"register.{p['register']}",
            value=float(p["raw"]),
            ts_ms=p["poll_ts_ms"],
            seq=p["poll_seq"],
            source=envelope.source,
            received_at_ms=envelope.received_at_ms,
        )
