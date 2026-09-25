"""有界缓存：容量耗尽、去重（含已确认数据的重试）、按序补发与确认截断。"""
from __future__ import annotations

import logging

from gateway.buffer import SpoolBuffer, StoreResult
from tests.conftest import NOW_MS

logger = logging.getLogger("gateway.test")


def pt(device="d1", metric="temp", seq=0, value=1.0):
    from gateway.models import TelemetryPoint
    return TelemetryPoint(device, metric, float(value), NOW_MS, seq, "http-json", NOW_MS)


def test_full_does_not_silently_drop():
    buf = SpoolBuffer(capacity=2)
    assert buf.store(pt(seq=0)) is StoreResult.STORED
    assert buf.store(pt(seq=1)) is StoreResult.STORED
    third = buf.store(pt(seq=2))
    logger.info("third store -> %s, len=%d capacity=%d", third, len(buf), buf.capacity)
    assert third is StoreResult.FULL
    assert len(buf) == 2  # 不覆盖、不静默丢弃


def test_duplicate_pending_is_idempotent():
    buf = SpoolBuffer(capacity=4)
    assert buf.store(pt(seq=0)) is StoreResult.STORED
    assert buf.store(pt(seq=0)) is StoreResult.DUPLICATE
    assert len(buf) == 1


def test_duplicate_after_ack_still_detected():
    buf = SpoolBuffer(capacity=4)
    buf.store(pt(seq=0))
    buf.ack_through(0)
    assert len(buf) == 0
    # 同一条数据因上游重投再次到达，仍应识别为重复
    assert buf.store(pt(seq=0)) is StoreResult.DUPLICATE


def test_pending_replay_in_offset_order_and_ack():
    buf = SpoolBuffer(capacity=4)
    buf.store(pt(seq=2))
    buf.store(pt(seq=0))
    pending = buf.pending()
    offsets = [r.offset for r in pending]
    logger.info("replay offsets (must be ascending): %s", offsets)
    assert offsets == sorted(offsets) == [0, 1]
    removed = buf.ack_through(0)  # 只确认第一条
    logger.info("acked through 0: removed=%d remaining=%s",
                removed, [r.offset for r in buf.pending()])
    assert removed == 1
    assert [r.offset for r in buf.pending()] == [1]
