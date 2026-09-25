import pytest

from gateway.models import RejectReason
from tests.conftest import env, http_payload, log_result


def test_http_json_normalized(gateway):
    gateway.sessions.open_connection("d1")
    result = gateway.ingest(env("http-json", http_payload("d1", 0)))
    log_result(result)
    assert result.accepted


def test_modbus_normalized(gateway):
    sessions = gateway.sessions
    sessions.open_connection("modbus-7")
    payload = {"unit": 7, "register": 4, "raw": 23,
               "poll_ts_ms": 1_800_000_000_000, "poll_seq": 0}
    result = gateway.ingest(env("modbus", payload))
    log_result(result)
    assert result.accepted
    assert result.point.device_id == "modbus-7"
    assert result.point.metric == "register.4"
    assert result.point.value == 23.0


def test_missing_field_rejected_with_origin(gateway):
    p = http_payload("d1", 0)
    del p["value"]
    result = gateway.ingest(env("http-json", p))
    log_result(result)
    assert not result.accepted
    assert result.rejection.reason is RejectReason.MISSING_FIELD
    # 原始出处必须保留
    assert result.rejection.envelope.payload == p
    assert "value" in result.rejection.detail


def test_bad_timestamp_unparseable(gateway):
    result = gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms="soon")))
    log_result(result)
    assert result.rejection.reason is RejectReason.BAD_TIMESTAMP


def test_bad_timestamp_out_of_skew(gateway):
    result = gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=0)))
    log_result(result)
    assert result.rejection.reason is RejectReason.BAD_TIMESTAMP


def test_unknown_source(gateway):
    result = gateway.ingest(env("mqtt-v9", http_payload("d1", 0)))
    log_result(result)
    assert result.rejection.reason is RejectReason.UNKNOWN_SOURCE
