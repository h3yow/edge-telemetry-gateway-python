"""端到端场景：下游故障恢复、重复投递、乱序到达、配额冲突。

验证：
- 下游不可用时数据可靠暂存，恢复后按到达顺序（offset）补发且仅一次；
- 重复投递幂等，补发流水无重复；
- 乱序到达按 offset 顺序补发；序列回退报文被明确拒绝；
- 配额/缓存耗尽冲突有可区分原因，不影响其他设备；
- 补发流水写入日志（forwarded offset=...）。
"""
from __future__ import annotations

import logging

from gateway.models import RejectReason
from tests.conftest import env, http_payload, log_result

logger = logging.getLogger("gateway.test")
BASE = 1_800_000_000_000


def _point_seq(record):
    return record.point.seq


def test_downstream_outage_buffered_then_replayed_in_order(gateway, sessions, downstream):
    sessions.open_connection("d1")
    downstream.set_up(False)
    for seq in range(3):
        r = gateway.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                               received_at_ms=BASE + seq))
        log_result(r)
        assert r.accepted
    assert len(downstream.received) == 0
    assert len(gateway.buffer) == 3

    # 恢复 -> flush 按序补发
    downstream.set_up(True)
    sent = gateway.flush()
    logger.info("replay stream: %s", [(r.offset, r.point.device_id, r.point.seq)
                                      for r in downstream.received])
    assert sent == 3
    assert list(map(_point_seq, downstream.received)) == [0, 1, 2]
    assert len(gateway.buffer) == 0


def test_duplicate_delivery_during_outage_replayed_once(gateway, sessions, downstream):
    sessions.open_connection("d1")
    downstream.set_up(False)
    gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE), received_at_ms=BASE))
    # 同一条报文因重试再次到达
    dup = gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE), received_at_ms=BASE + 5))
    log_result(dup)
    assert dup.accepted and dup.duplicate
    assert len(gateway.buffer) == 1

    downstream.set_up(True)
    gateway.flush()
    assert list(map(_point_seq, downstream.received)) == [0]  # 仅补发一次


def test_duplicate_replay_after_ack_is_not_resent(gateway, sessions, downstream):
    """补发确认后，历史报文重投：识别为重复，不再次进入补发流水。"""
    sessions.open_connection("d1")
    gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE), received_at_ms=BASE))
    assert list(map(_point_seq, downstream.received)) == [0]
    # 上游重投旧报文
    again = gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE),
                               received_at_ms=BASE + 100))
    log_result(again)
    assert again.accepted and again.duplicate
    assert list(map(_point_seq, downstream.received)) == [0]


def test_out_of_order_arrival_replayed_by_offset(gateway, sessions, downstream):
    """到达顺序与 seq 无关：补发严格按 offset（接收顺序）升序。"""
    sessions.open_connection("d1")
    downstream.set_up(False)
    # seq 严格递增但 ts 乱序到达（设备时钟波动）：接收顺序决定补发顺序
    for seq, ts, arr in [(0, BASE + 50, BASE), (1, BASE + 10, BASE + 1),
                         (2, BASE + 90, BASE + 2)]:
        r = gateway.ingest(env("http-json", http_payload("d1", seq, ts_ms=ts),
                               received_at_ms=arr))
        assert r.accepted
    downstream.set_up(True)
    gateway.flush()
    offsets = [r.offset for r in downstream.received]
    seqs = [r.point.seq for r in downstream.received]
    logger.info("out-of-order replay offsets=%s seqs=%s", offsets, seqs)
    assert offsets == sorted(offsets)
    assert seqs == [0, 1, 2]  # seq 恰好按到达顺序给出


def test_sequence_regression_during_outage_is_rejected(gateway, sessions, downstream):
    sessions.open_connection("d1")
    downstream.set_up(False)
    gateway.ingest(env("http-json", http_payload("d1", 1, ts_ms=BASE), received_at_ms=BASE))
    bad = gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE + 1),
                             received_at_ms=BASE + 1))
    log_result(bad)
    assert bad.rejection.reason is RejectReason.SEQUENCE_REGRESSION
    assert len(gateway.buffer) == 1


def test_buffer_full_rejected_then_drains_after_recovery(gateway, sessions, downstream):
    sessions.open_connection("d1")
    downstream.set_up(False)
    # 间隔超过频率窗口，避免触发频率配额，专注验证缓存容量
    for seq in range(4):  # capacity = 4
        assert gateway.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq * 2000),
                                  received_at_ms=BASE + seq * 2000)).accepted
    full = gateway.ingest(env("http-json", http_payload("d1", 4, ts_ms=BASE + 8000),
                              received_at_ms=BASE + 8000))
    log_result(full)
    assert full.rejection.reason is RejectReason.BUFFER_FULL
    # 恢复并补发后，空间释放，新报文可被接受
    downstream.set_up(True)
    gateway.flush()
    retry = gateway.ingest(env("http-json", http_payload("d1", 4, ts_ms=BASE + 8000),
                               received_at_ms=BASE + 8001))
    assert retry.accepted
    assert list(map(_point_seq, downstream.received)) == [0, 1, 2, 3, 4]


def test_quota_conflict_does_not_affect_other_device(gateway, sessions, downstream):
    sessions.open_connection("d1")
    sessions.open_connection("d2")
    base = BASE
    for seq in range(3):
        gateway.ingest(env("http-json", http_payload("d1", seq, ts_ms=base + seq),
                           received_at_ms=base + seq))
    blocked = gateway.ingest(env("http-json", http_payload("d1", 3, ts_ms=base + 3),
                                 received_at_ms=base + 3))
    log_result(blocked)
    assert blocked.rejection.reason is RejectReason.QUOTA_EXCEEDED
    fine = gateway.ingest(env("http-json", http_payload("d2", 0, ts_ms=base),
                              received_at_ms=base))
    assert fine.accepted
    # d1 的 3 条窗口内数据正常转发，第 4 条被拒；d2 的数据不受影响
    assert list(map(_point_seq, downstream.received)) == [0, 1, 2, 0]


def test_interleaved_devices_replay_preserves_global_order(gateway, sessions, downstream):
    sessions.open_connection("d1")
    sessions.open_connection("d2")
    downstream.set_up(False)
    gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE), received_at_ms=BASE))
    gateway.ingest(env("http-json", http_payload("d2", 0, ts_ms=BASE), received_at_ms=BASE + 1))
    gateway.ingest(env("http-json", http_payload("d1", 1, ts_ms=BASE + 1),
                       received_at_ms=BASE + 2))
    downstream.set_up(True)
    gateway.flush()
    stream = [(r.offset, r.point.device_id, r.point.seq) for r in downstream.received]
    logger.info("interleaved replay stream: %s", stream)
    assert [d for _, d, _ in stream] == ["d1", "d2", "d1"]
    assert [o for o, _, _ in stream] == [0, 1, 2]


def test_replay_flow_is_logged(gateway, sessions, downstream, caplog):
    sessions.open_connection("d1")
    downstream.set_up(False)
    gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE), received_at_ms=BASE))
    downstream.set_up(True)
    gateway.flush()
    forwarded = [r.message for r in caplog.records if "forwarded offset=" in r.message]
    logger.info("forwarded log lines: %s", forwarded)
    assert forwarded and "offset=0" in forwarded[0]
