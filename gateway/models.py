"""统一内部数据模型与拒绝原因定义（骨架）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class RejectReason(enum.Enum):
    """报文被拒绝的可区分原因。"""

    MISSING_FIELD = "missing_field"            # 必填字段缺失
    BAD_TIMESTAMP = "bad_timestamp"            # 时间戳异常（不可解析/超出容忍范围）
    SEQUENCE_REGRESSION = "sequence_regression"  # 同一设备上报序列回退
    QUOTA_EXCEEDED = "quota_exceeded"          # 超出接入配额（并发/频率）
    SESSION_CONFLICT = "session_conflict"      # 会话状态冲突
    BUFFER_FULL = "buffer_full"                # 暂存空间耗尽
    UNKNOWN_SOURCE = "unknown_source"          # 未注册的接入来源


@dataclass(frozen=True)
class RawEnvelope:
    """接入层收到的原始报文及其出处。"""

    source: str                 # 来源协议/通道标识，如 "modbus" / "http-json"
    payload: dict[str, Any]     # 原始报文体
    received_at_ms: int         # 网关接收时刻（毫秒）


@dataclass(frozen=True)
class TelemetryPoint:
    """归一化后的内部数据模型。"""

    device_id: str
    metric: str
    value: float
    ts_ms: int                  # 设备侧时间戳（毫秒，归一化后）
    seq: int                    # 设备侧单调序列号
    source: str                 # 原始出处（协议/通道）
    received_at_ms: int         # 网关接收时刻


@dataclass(frozen=True)
class Rejection:
    """被拒绝报文的记录：原因 + 原始出处，便于解释与审计。"""

    reason: RejectReason
    envelope: RawEnvelope
    detail: str = ""


@dataclass(frozen=True)
class IngestResult:
    """单条报文的接入判定结果。"""

    accepted: bool
    point: TelemetryPoint | None = None
    rejection: Rejection | None = None
    duplicate: bool = False     # 重复投递被识别（幂等接受，不重复入缓存）
