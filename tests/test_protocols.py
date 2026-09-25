"""协议归一：多协议解析与可区分拒绝原因。"""

from gateway.models import RejectReason

from .conftest import NOW, log_decision, log_input


def test_json_csv_kv_normalize_to_same_model(make_gateway):
    g = make_gateway()
    g.open_session("d1", "s1")
    cases = [
        ('{"device":"d1","seq":1,"ts":%d,"metrics":{"t":20.5}}' % NOW, "json"),
        ("d1,2,%d,t=20.5" % NOW, "csv"),
        ("device=d1;seq=3;ts=%d;t=20.5" % NOW, "kv"),
    ]
    for raw, proto in cases:
        log_input(raw, proto, "d1", "s1", NOW)
        r = g.ingest(raw, proto, "d1", "s1", NOW)
        log_decision(r)
        assert r.accepted
        assert r.record.metrics == {"t": 20.5}
        assert r.record.source_protocol == proto
        assert r.record.raw == raw


def test_missing_field_rejected_with_origin(make_gateway):
    g = make_gateway()
    g.open_session("d1", "s1")
    raw = '{"seq":1,"ts":%d}' % NOW
    log_input(raw, "json", "d1", "s1", NOW)
    r = g.ingest(raw, "json", "d1", "s1", NOW)
    log_decision(r)
    assert not r.accepted
    assert r.reason is RejectReason.MISSING_FIELD
    assert "device" in r.detail
    assert r.raw == raw and r.source_protocol == "json"


def test_timestamp_anomaly_future_and_past(make_gateway):
    g = make_gateway()
    g.open_session("d1", "s1")
    for ts in (NOW + 10 * 60 * 1000, NOW - 31 * 24 * 3600 * 1000):
        raw = '{"device":"d1","seq":1,"ts":%d}' % ts
        log_input(raw, "json", "d1", "s1", NOW)
        r = g.ingest(raw, "json", "d1", "s1", NOW)
        log_decision(r)
        assert not r.accepted
        assert r.reason is RejectReason.TIMESTAMP_ANOMALY


def test_malformed_and_unknown_protocol(make_gateway):
    g = make_gateway()
    g.open_session("d1", "s1")
    raw = "not,a,valid,json,packet"
    log_input(raw, "json", "d1", "s1", NOW)
    r = g.ingest(raw, "json", "d1", "s1", NOW)
    log_decision(r)
    assert r.reason is RejectReason.MALFORMED_PAYLOAD

    log_input(raw, "mqtt", "d1", "s1", NOW)
    r = g.ingest(raw, "mqtt", "d1", "s1", NOW)
    log_decision(r)
    assert r.reason is RejectReason.UNKNOWN_PROTOCOL
