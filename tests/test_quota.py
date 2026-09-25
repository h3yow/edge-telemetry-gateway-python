"""配额与会话冲突：频率上限、并发连接、设备隔离、配置版本语义。"""

import pytest

from gateway import QuotaConfig
from gateway.models import RejectReason

from .conftest import NOW, log_decision, log_input


def _ingest(g, seq, device="d1", session="s1", now=NOW):
    raw = '{"device":"%s","seq":%d,"ts":%d}' % (device, seq, now)
    log_input(raw, "json", device, session, now)
    r = g.ingest(raw, "json", device, session, now)
    log_decision(r)
    return r


def test_rate_quota_exceeded_and_window_slides(make_gateway):
    g = make_gateway(QuotaConfig(max_reports_per_window=3, window_ms=1000))
    g.open_session("d1", "s1")
    for seq in (1, 2, 3):
        assert _ingest(g, seq).accepted
    r = _ingest(g, 4)
    assert r.reason is RejectReason.QUOTA_EXCEEDED

    print("[时间] 窗口滑动 1001ms")
    assert _ingest(g, 4, now=NOW + 1001).accepted  # 窗口滑出后同号重发可受理


def test_concurrent_connection_limit(make_gateway):
    g = make_gateway(QuotaConfig(max_connections_per_device=1))
    assert g.open_session("d1", "s1").accepted
    r = g.open_session("d1", "s2")
    print(f"[判定] 拒绝 reason={r.reason.value} detail={r.detail}")
    assert r.reason is RejectReason.SESSION_CONFLICT

    g.close_session("d1", "s1")
    assert g.open_session("d1", "s2").accepted  # 释放后可再接入


def test_report_without_active_session_rejected(make_gateway):
    g = make_gateway()
    r = _ingest(g, 1, session="ghost")
    assert r.reason is RejectReason.SESSION_CONFLICT


def test_quota_conflict_does_not_affect_other_devices(make_gateway):
    g = make_gateway(QuotaConfig(max_reports_per_window=1, window_ms=60_000))
    g.open_session("d1", "s1")
    g.open_session("d2", "s2")
    assert _ingest(g, 1, device="d1", session="s1").accepted
    r = _ingest(g, 2, device="d1", session="s1")
    assert r.reason is RejectReason.QUOTA_EXCEEDED   # d1 被限流
    assert _ingest(g, 1, device="d2", session="s2").accepted  # d2 不受影响


def test_config_change_versioned_and_reproducible(make_gateway):
    """配置版本严格递增；历史记录保留归一时的版本，语义可复现。"""
    g = make_gateway(QuotaConfig(version=1, max_reports_per_window=1,
                                 window_ms=60_000))
    g.open_session("d1", "s1")
    r = _ingest(g, 1)
    assert r.record.config_version == 1
    assert _ingest(g, 2).reason is RejectReason.QUOTA_EXCEEDED

    print("[配置] 放宽配额 version 1 -> 2")
    g.update_quota(QuotaConfig(version=2, max_reports_per_window=10,
                               window_ms=60_000))
    r = _ingest(g, 2)
    assert r.accepted and r.record.config_version == 2

    # 历史记录仍是版本 1 的语义，新记录是版本 2，可区分复现
    versions = [rec.config_version for rec in g.downstream.received]
    print(f"[判定] 下游记录配置版本序列: {versions}")
    assert versions == [1, 2]

    with pytest.raises(ValueError):
        g.update_quota(QuotaConfig(version=2))  # 版本不递增被拒绝
