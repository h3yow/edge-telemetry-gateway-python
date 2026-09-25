"""设备会话与接入配额：会话冲突、并发配额、序列回退、频率配额、配置变更。"""
from __future__ import annotations

import logging

from gateway.models import RejectReason
from gateway.session import QuotaPolicy
from tests.conftest import env, http_payload, log_result

logger = logging.getLogger("gateway.test")


def test_report_without_session_conflict(gateway):
    result = gateway.ingest(env("http-json", http_payload("d1", 0)))
    log_result(result)
    assert result.rejection.reason is RejectReason.SESSION_CONFLICT


def test_concurrent_connection_quota_is_per_device(gateway, sessions):
    assert sessions.open_connection("d1") is None
    assert sessions.open_connection("d1") is RejectReason.QUOTA_EXCEEDED
    # 不影响其他设备
    assert sessions.open_connection("d2") is None


def test_sequence_regression_and_duplicate_seq(gateway, sessions):
    sessions.open_connection("d1")
    r1 = gateway.ingest(env("http-json", http_payload("d1", 1)))
    r2 = gateway.ingest(env("http-json", http_payload("d1", 0)))  # 回退
    # 同 seq 但内容不同（ts 变了）-> 序列回退；完全相同的重投 -> 重复幂等（见场景测试）
    r3 = gateway.ingest(env("http-json", http_payload("d1", 1, ts_ms=1_800_000_000_009)))
    log_result(r2); log_result(r3)
    assert r1.accepted
    assert r2.rejection.reason is RejectReason.SEQUENCE_REGRESSION
    assert r3.rejection.reason is RejectReason.SEQUENCE_REGRESSION


def test_rate_quota_rejects_only_offending_device(gateway, sessions):
    sessions.open_connection("d1")
    sessions.open_connection("d2")
    base = 1_800_000_000_000
    for seq in range(3):  # 窗口配额 = 3
        r = gateway.ingest(env("http-json", http_payload("d1", seq, ts_ms=base), received_at_ms=base))
        assert r.accepted
    over = gateway.ingest(env("http-json", http_payload("d1", 3, ts_ms=base), received_at_ms=base))
    log_result(over)
    assert over.rejection.reason is RejectReason.QUOTA_EXCEEDED
    # 其他设备不受影响
    other = gateway.ingest(env("http-json", http_payload("d2", 0, ts_ms=base), received_at_ms=base))
    assert other.accepted
    # 窗口滑动后重新允许
    later = gateway.ingest(env("http-json", http_payload("d1", 4, ts_ms=base + 1001),
                               received_at_ms=base + 1001))
    assert later.accepted


def test_policy_update_resets_window_but_keeps_sequence(gateway, sessions):
    sessions.open_connection("d1")
    base = 1_800_000_000_000
    gateway.ingest(env("http-json", http_payload("d1", 1, ts_ms=base), received_at_ms=base))
    # 新版本配置：更高频率
    sessions.update_policy(QuotaPolicy(version=2, max_concurrent_sessions_per_device=1,
                                       max_reports_per_window=10, window_ms=1000))
    # 序列连续性保留：seq=1 仍视为回退
    regress = gateway.ingest(env("http-json", http_payload("d1", 1, ts_ms=base + 1),
                                 received_at_ms=base + 1))
    log_result(regress)
    assert regress.rejection.reason is RejectReason.SEQUENCE_REGRESSION
    # 新窗口配额生效
    ok = gateway.ingest(env("http-json", http_payload("d1", 2, ts_ms=base + 2),
                            received_at_ms=base + 2))
    assert ok.accepted
    assert sessions.session_of("d1").policy_version == 2
