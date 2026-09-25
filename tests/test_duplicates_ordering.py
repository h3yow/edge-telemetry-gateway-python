"""重复与乱序上报：序列回退拒绝、重复投递识别、乱序恢复。"""

from gateway.models import RejectReason

from .conftest import NOW, log_decision, log_input, log_replay


def _ingest(g, seq, device="d1", session="s1", now=NOW):
    raw = '{"device":"%s","seq":%d,"ts":%d}' % (device, seq, now)
    log_input(raw, "json", device, session, now)
    r = g.ingest(raw, "json", device, session, now)
    log_decision(r)
    return r


def test_sequence_regression_rejected_with_reason(make_gateway):
    g = make_gateway()
    g.open_session("d1", "s1")
    assert _ingest(g, 1).accepted
    assert _ingest(g, 2).accepted

    r = _ingest(g, 2)  # 同号重发
    assert not r.accepted and r.reason is RejectReason.SEQUENCE_REGRESSION

    r = _ingest(g, 1)  # 回退到更早
    assert not r.accepted and r.reason is RejectReason.SEQUENCE_REGRESSION

    assert _ingest(g, 3).accepted  # 正常递增不受影响
    assert [r.seq for r in g.downstream.received] == [1, 2, 3]


def test_out_of_order_report_does_not_corrupt_state(make_gateway):
    """乱序到达：回退的被拒，后续正确序列仍按序前进，状态不被污染。"""
    g = make_gateway()
    g.open_session("d1", "s1")
    assert _ingest(g, 5).accepted          # 设备重启后从 5 开始也允许（首条不设限）
    r = _ingest(g, 4)                       # 迟到的旧报文
    assert r.reason is RejectReason.SEQUENCE_REGRESSION
    assert _ingest(g, 6).accepted
    assert [r.seq for r in g.downstream.received] == [5, 6]


def test_duplicate_delivery_during_outage(make_gateway):
    """下游故障期间同一记录重复入队被识别为 DUPLICATE，恢复后只补发一份。"""
    g = make_gateway()
    g.open_session("d1", "s1")
    g.set_downstream_available(False, NOW)
    assert _ingest(g, 1).accepted

    # 模拟发送方重试：直接对暂存队列重复入队同一记录
    from gateway.models import TelemetryRecord
    dup = TelemetryRecord("d1", 1, NOW, {}, "json", "raw-dup")
    print(f"[输入] 发送方重试，重复入队 device=d1 seq=1")
    err = g.buffer.enqueue(dup)
    print(f"[判定] 拒绝 reason=duplicate err={err}")
    assert err == "duplicate"
    assert len(g.buffer) == 1

    flow = g.set_downstream_available(True, NOW)
    log_replay(flow)
    assert [r.seq for r in g.downstream.received] == [1]  # 只补发一份


def test_rejected_record_does_not_consume_sequence(make_gateway):
    """被暂存满拒绝的报文不消耗序列号，腾出空间后同号重发可受理。"""
    g = make_gateway(capacity=1)
    g.open_session("d1", "s1")
    g.set_downstream_available(False, NOW)
    assert _ingest(g, 1).accepted
    r = _ingest(g, 2)
    assert r.reason is RejectReason.BUFFER_FULL

    g.set_downstream_available(True, NOW)   # 补发清空
    g.set_downstream_available(False, NOW)
    assert _ingest(g, 2).accepted           # 同号重发成功
