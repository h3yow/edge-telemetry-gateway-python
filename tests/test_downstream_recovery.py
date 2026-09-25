"""下游故障恢复：暂存、按序补发、补发中断保留、暂存耗尽。"""

from gateway.models import RejectReason

from .conftest import NOW, log_decision, log_input, log_replay


def _ingest(g, seq, device="d1", session="s1", now=NOW):
    raw = '{"device":"%s","seq":%d,"ts":%d}' % (device, seq, now)
    log_input(raw, "json", device, session, now)
    r = g.ingest(raw, "json", device, session, now)
    log_decision(r)
    return r


def test_outage_buffers_then_replays_in_order(make_gateway):
    g = make_gateway()
    g.open_session("d1", "s1")

    assert _ingest(g, 1).detail == "delivered"  # 下游正常，直达

    print("[故障] 下游置为不可用")
    g.set_downstream_available(False, NOW)
    for seq in (2, 3, 4):
        assert _ingest(g, seq).detail == "buffered"
    assert len(g.buffer) == 3

    print("[恢复] 下游恢复，触发补发")
    flow = g.set_downstream_available(True, NOW)
    log_replay(flow)
    assert [f.record.seq for f in flow] == [2, 3, 4]  # 严格按入队顺序
    assert all(f.delivered for f in flow)
    assert [r.seq for r in g.downstream.received] == [1, 2, 3, 4]
    assert len(g.buffer) == 0


def test_replay_stops_on_failure_and_keeps_remainder(make_gateway):
    g = make_gateway()
    g.open_session("d1", "s1")
    g.set_downstream_available(False, NOW)
    for seq in (1, 2, 3):
        _ingest(g, seq)

    # 恢复后第一条补发即再次失败（下游投递时仍抖动）
    calls = {"n": 0}
    original = g.downstream.deliver

    def flaky(record):
        calls["n"] += 1
        if calls["n"] == 1:
            return False
        return original(record)

    print("[恢复] 下游恢复但首次投递抖动")
    flow = g.buffer.replay(flaky, NOW)
    log_replay(flow)
    assert len(flow) == 1 and not flow[0].delivered
    assert [r.seq for r in g.buffer.pending()] == [1, 2, 3]  # 剩余完整保留

    print("[恢复] 再次补发成功")
    flow = g.set_downstream_available(True, NOW)
    log_replay(flow)
    assert [r.seq for r in g.downstream.received] == [1, 2, 3]


def test_buffer_full_rejected_not_silent(make_gateway):
    g = make_gateway(capacity=2)
    g.open_session("d1", "s1")
    g.set_downstream_available(False, NOW)
    assert _ingest(g, 1).accepted
    assert _ingest(g, 2).accepted
    r = _ingest(g, 3)
    assert not r.accepted
    assert r.reason is RejectReason.BUFFER_FULL
    assert len(g.buffer) == 2  # 容量有界，内存不无上限增长

    print("[恢复] 补发后腾出空间可继续暂存")
    g.set_downstream_available(True, NOW)
    g.set_downstream_available(False, NOW)
    assert _ingest(g, 3).accepted


def test_no_silent_loss_across_outage(make_gateway):
    """故障期间接受数 + 拒绝数 = 提交数，恢复后下游收到全部接受的数据。"""
    g = make_gateway(capacity=3)
    g.open_session("d1", "s1")
    g.set_downstream_available(False, NOW)
    accepted = rejected = 0
    for seq in range(1, 6):
        if _ingest(g, seq).accepted:
            accepted += 1
        else:
            rejected += 1
    assert accepted == 3 and rejected == 2
    flow = g.set_downstream_available(True, NOW)
    log_replay(flow)
    assert [r.seq for r in g.downstream.received] == [1, 2, 3]
