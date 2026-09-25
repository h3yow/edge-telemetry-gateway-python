"""网关编排：归一 -> 会话/配额 -> 缓存 -> 转发/补发（骨架）。"""
from __future__ import annotations

from .buffer import SpoolBuffer
from .downstream import Downstream
from .models import IngestResult, RawEnvelope
from .normalizer import Normalizer
from .session import SessionManager


class Gateway:
    def __init__(
        self,
        normalizer: Normalizer,
        sessions: SessionManager,
        buffer: SpoolBuffer,
        downstream: Downstream,
    ) -> None:
        self._normalizer = normalizer
        self._sessions = sessions
        self._buffer = buffer
        self._downstream = downstream

    def ingest(self, envelope: RawEnvelope) -> IngestResult:
        """接入单条原始报文，返回可区分的判定结果。"""
        raise NotImplementedError

    def flush(self) -> int:
        """下游可用时按序补发缓存数据，返回补发条数。"""
        raise NotImplementedError
