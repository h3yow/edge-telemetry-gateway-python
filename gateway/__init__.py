"""边缘数据接入网关：多协议归一、会话配额、本地缓存与补发。"""

from .models import (
    RejectReason,
    TelemetryRecord,
    IngestResult,
    ForwardResult,
)
from .protocols import normalize
from .session import SessionManager, QuotaConfig
from .buffer import ReplayBuffer
from .downstream import SimulatedDownstream
from .gateway import Gateway

__all__ = [
    "RejectReason",
    "TelemetryRecord",
    "IngestResult",
    "ForwardResult",
    "normalize",
    "SessionManager",
    "QuotaConfig",
    "ReplayBuffer",
    "SimulatedDownstream",
    "Gateway",
]
