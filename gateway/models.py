"""统一内部数据模型与判定结果类型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Optional


class RejectReason(enum.Enum):
    """可区分的拒绝原因。"""

    MISSING_FIELD = "missing_field"            # 必填字段缺失
    TIMESTAMP_ANOMALY = "timestamp_anomaly"    # 时间戳异常（未来过远/过旧/非法）
    SEQUENCE_REGRESSION = "sequence_regression"  # 同设备上报序列回退
    QUOTA_EXCEEDED = "quota_exceeded"          # 超出上报频率配额
    SESSION_CONFLICT = "session_conflict"      # 并发连接数超限或会话状态冲突
    BUFFER_FULL = "buffer_full"                # 暂存空间耗尽
    DUPLICATE = "duplicate"                    # 重复投递（已接收过）
    UNKNOWN_PROTOCOL = "unknown_protocol"      # 无法识别的接入协议
    MALFORMED_PAYLOAD = "malformed_payload"    # 报文无法解析


@dataclass(frozen=True)
class TelemetryRecord:
    """归一后的内部数据模型。"""

    device_id: str
    seq: int
    timestamp_ms: int
    metrics: dict[str, float]
    source_protocol: str          # 原始出处：协议名
    raw: str                      # 原始出处：原始报文
    config_version: int = 0       # 归一所用配置版本，保证历史语义可复现


@dataclass(frozen=True)
class IngestResult:
    """接入判定结果：成功或带原因/出处的拒绝。"""

    accepted: bool
    record: Optional[TelemetryRecord] = None
    reason: Optional[RejectReason] = None
    detail: str = ""
    raw: str = ""
    source_protocol: str = ""

    @staticmethod
    def ok(record: TelemetryRecord) -> "IngestResult":
        return IngestResult(accepted=True, record=record)

    @staticmethod
    def reject(reason: RejectReason, detail: str,
               raw: str = "", source_protocol: str = "") -> "IngestResult":
        return IngestResult(accepted=False, reason=reason, detail=detail,
                            raw=raw, source_protocol=source_protocol)


@dataclass(frozen=True)
class ForwardResult:
    """转发/补发流水记录。"""

    record: TelemetryRecord
    delivered: bool
    attempt: int
    note: str = ""
