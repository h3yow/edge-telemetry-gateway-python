"""网关编排：归一 -> 会话/配额 -> 缓存 -> 转发/补发。

数据通路（确定性）：
1. 归一化失败 -> 拒绝（含原因与原始出处）；
2. 会话/配额校验失败 -> 拒绝，不影响其他设备；
3. 入缓存：重复 -> 幂等接受（duplicate=True）；满 -> BUFFER_FULL 拒绝；
4. 入缓存成功后尝试按序转发；下游不可用时保留在缓存，恢复后 flush 按序补发。
"""
from __future__ import annotations

import logging

from .buffer import SpoolBuffer, StoreResult
from .downstream import Downstream
from .models import IngestResult, RawEnvelope, Rejection, RejectReason
from .normalizer import Normalizer
from .session import SessionManager

logger = logging.getLogger("gateway")


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

    @property
    def buffer(self) -> SpoolBuffer:
        return self._buffer

    @property
    def sessions(self) -> SessionManager:
        return self._sessions

    def ingest(self, envelope: RawEnvelope) -> IngestResult:
        """接入单条原始报文，返回可区分的判定结果。"""
        normalized = self._normalizer.normalize(envelope)
        if isinstance(normalized, Rejection):
            logger.info("reject %s: %s", normalized.reason.value, normalized.detail)
            return IngestResult(accepted=False, rejection=normalized)
        point = normalized

        reason = self._sessions.check(point)
        if reason is not None:
            rej = Rejection(reason=reason, envelope=envelope,
                            detail=f"device={point.device_id} seq={point.seq}")
            logger.info("reject %s: %s", rej.reason.value, rej.detail)
            return IngestResult(accepted=False, rejection=rej)

        stored = self._buffer.store(point)
        if stored is StoreResult.DUPLICATE:
            logger.info("duplicate device=%s metric=%s seq=%s (idempotent accept)",
                        point.device_id, point.metric, point.seq)
            return IngestResult(accepted=True, point=point, duplicate=True)
        if stored is StoreResult.FULL:
            rej = Rejection(reason=RejectReason.BUFFER_FULL, envelope=envelope,
                            detail=f"buffer capacity {self._buffer.capacity} exhausted")
            logger.info("reject %s: %s", rej.reason.value, rej.detail)
            return IngestResult(accepted=False, rejection=rej)

        logger.info("stored device=%s metric=%s seq=%s", point.device_id, point.metric, point.seq)
        self.flush()
        return IngestResult(accepted=True, point=point)

    def flush(self) -> int:
        """下游可用时按序补发缓存数据，返回补发条数。"""
        sent = 0
        for rec in self._buffer.pending():
            if self._downstream.send(rec):
                self._buffer.ack_through(rec.offset)
                sent += 1
                logger.info("forwarded offset=%d device=%s seq=%d",
                            rec.offset, rec.point.device_id, rec.point.seq)
            else:
                logger.info("downstream unavailable, %d record(s) remain buffered",
                            len(self._buffer))
                break
        return sent
