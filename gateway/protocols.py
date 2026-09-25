"""多协议报文归一：把不同来源的报文解析为 TelemetryRecord。

支持三种本地协议格式（均归一到同一内部模型）：
- json    : {"device": "d1", "seq": 1, "ts": 1700000000000, "metrics": {"t": 20.5}}
- csv     : device,seq,ts,key=value,key=value        例: d1,1,1700000000000,t=20.5,h=0.4
- kv      : device=d1;seq=1;ts=1700000000000;t=20.5
"""

from __future__ import annotations

import json

from .models import IngestResult, RejectReason, TelemetryRecord

# 时间戳合法窗口：允许的未来偏移与最大过去偏移（毫秒）
MAX_FUTURE_SKEW_MS = 5 * 60 * 1000
MAX_PAST_SKEW_MS = 30 * 24 * 3600 * 1000

_REQUIRED = ("device", "seq", "ts")


def _parse_fields(raw: str, protocol: str) -> dict[str, str] | None:
    """按协议把原始报文解析为字段字典；无法解析返回 None。"""
    try:
        if protocol == "json":
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                return None
            fields: dict[str, str] = {}
            for k, v in obj.items():
                if k == "metrics" and isinstance(v, dict):
                    for mk, mv in v.items():
                        fields[str(mk)] = str(mv)
                else:
                    fields[str(k)] = str(v)
            return fields
        if protocol == "csv":
            parts = [p.strip() for p in raw.split(",")]
            if len(parts) < 3:
                return None
            fields = {"device": parts[0], "seq": parts[1], "ts": parts[2]}
            for p in parts[3:]:
                if "=" not in p:
                    return None
                k, v = p.split("=", 1)
                fields[k.strip()] = v.strip()
            return fields
        if protocol == "kv":
            fields = {}
            for p in raw.split(";"):
                p = p.strip()
                if not p:
                    continue
                if "=" not in p:
                    return None
                k, v = p.split("=", 1)
                fields[k.strip()] = v.strip()
            return fields
    except (ValueError, TypeError):
        return None
    return None  # 未知协议


def normalize(raw: str, protocol: str, now_ms: int,
              config_version: int = 0) -> IngestResult:
    """把原始报文归一到内部模型；失败时给出可区分原因与出处。"""
    if protocol not in ("json", "csv", "kv"):
        return IngestResult.reject(
            RejectReason.UNKNOWN_PROTOCOL,
            f"不支持的协议: {protocol!r}", raw=raw, source_protocol=protocol)

    fields = _parse_fields(raw, protocol)
    if fields is None:
        return IngestResult.reject(
            RejectReason.MALFORMED_PAYLOAD,
            "报文无法按协议解析", raw=raw, source_protocol=protocol)

    for name in _REQUIRED:
        if not fields.get(name):
            return IngestResult.reject(
                RejectReason.MISSING_FIELD,
                f"必填字段缺失: {name}", raw=raw, source_protocol=protocol)

    try:
        seq = int(fields["seq"])
        ts = int(fields["ts"])
    except ValueError:
        return IngestResult.reject(
            RejectReason.MALFORMED_PAYLOAD,
            "seq/ts 不是合法整数", raw=raw, source_protocol=protocol)

    if ts > now_ms + MAX_FUTURE_SKEW_MS:
        return IngestResult.reject(
            RejectReason.TIMESTAMP_ANOMALY,
            f"时间戳超前 {ts - now_ms}ms 超过允许偏移 {MAX_FUTURE_SKEW_MS}ms",
            raw=raw, source_protocol=protocol)
    if ts < now_ms - MAX_PAST_SKEW_MS:
        return IngestResult.reject(
            RejectReason.TIMESTAMP_ANOMALY,
            f"时间戳落后 {now_ms - ts}ms 超过允许窗口 {MAX_PAST_SKEW_MS}ms",
            raw=raw, source_protocol=protocol)

    metrics: dict[str, float] = {}
    for k, v in fields.items():
        if k in _REQUIRED:
            continue
        try:
            metrics[k] = float(v)
        except ValueError:
            return IngestResult.reject(
                RejectReason.MALFORMED_PAYLOAD,
                f"指标 {k!r} 的值不是数值: {v!r}",
                raw=raw, source_protocol=protocol)

    record = TelemetryRecord(
        device_id=fields["device"], seq=seq, timestamp_ms=ts,
        metrics=metrics, source_protocol=protocol, raw=raw,
        config_version=config_version)
    return IngestResult.ok(record)
