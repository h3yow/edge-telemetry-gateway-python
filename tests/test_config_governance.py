"""配置治理 + 判定追溯。

验证：
- 按设备差异化配置：不同设备不同的并发/频率上限，互不影响；
- 运行期热更新：改完立即对目标设备生效，前后判定对照可查；
- 判定追溯：每条判定钉住当时生效的配置版本，可按设备/时间段查询；
- 重放：把历史记录原样重放，判定结论与当时完全一致；
- 已受理未转发的数据：改配置后补发不按新配置重判，顺序不变；
- 并发：配置切换与上报/补发同时进行，无重复判定、无丢失、顺序不乱；
- 非法配置/版本倒退：整批拒绝，原配置不变；
- 追溯日志有界：超出容量淘汰最旧并计数。
"""
from __future__ import annotations

import logging
import threading
from types import SimpleNamespace

import pytest

from gateway.audit import AuditLog, Decision, EntryKind, replay
from gateway.buffer import SpoolBuffer
from gateway.config import (ConfigError, ConfigRegistry, ConfigUpdate, DeviceConfig)
from gateway.downstream import SimulatedDownstream
from gateway.gateway import Gateway
from gateway.models import RejectReason
from gateway.normalizer import HttpJsonAdapter, ModbusAdapter, Normalizer
from gateway.session import QuotaPolicy, SessionManager
from tests.conftest import env, http_payload, log_result

logger = logging.getLogger("gateway.test")
BASE = 1_800_000_000_000

# 默认配置：并发 2、窗口 1000ms 内最多 3 条
DEFAULT_CFG = DeviceConfig(max_concurrent_sessions_per_device=2,
                           max_reports_per_window=3, window_ms=1000)


@pytest.fixture
def gov() -> SimpleNamespace:
    normalizer = Normalizer()
    normalizer.register(HttpJsonAdapter())
    normalizer.register(ModbusAdapter())
    sessions = SessionManager(QuotaPolicy(
        version=1, max_concurrent_sessions_per_device=2,
        max_reports_per_window=3, window_ms=1000))
    configs = ConfigRegistry(DEFAULT_CFG, version=1, history_limit=8)
    audit = AuditLog(capacity=256)
    downstream = SimulatedDownstream()
    gw = Gateway(normalizer, sessions, SpoolBuffer(capacity=64), downstream,
                 config_registry=configs, audit_log=audit)
    return SimpleNamespace(gw=gw, sessions=sessions, configs=configs,
                           audit=audit, downstream=downstream)


def _decisions(audit: AuditLog, device: str):
    return [e for e in audit.query(device_id=device) if e.kind is EntryKind.DECISION]


def test_per_device_differential_config(gov):
    """同一注册表下，d1 放宽到 5 条/窗，d2 收紧到 1 条/窗，其余设备用默认 3 条。"""
    gov.configs.apply(ConfigUpdate(version=2, devices={
        "d1": DeviceConfig(2, 5, 1000),
        "d2": DeviceConfig(2, 1, 1000),
    }))
    for dev in ("d1", "d2", "d3"):
        assert gov.gw.connect(dev) is None

    outcomes = {}
    for dev in ("d1", "d2", "d3"):
        res = []
        for seq in range(5):
            r = gov.gw.ingest(env("http-json", http_payload(dev, seq, ts_ms=BASE + seq),
                                  received_at_ms=BASE + seq))
            log_result(r)
            res.append(r.accepted and not r.duplicate)
        outcomes[dev] = res
    logger.info("differential outcomes: %s", outcomes)
    assert outcomes["d1"] == [True] * 5                    # 放宽：5 条全收
    assert outcomes["d2"] == [True, False, False, False, False]  # 收紧：只收 1 条
    assert outcomes["d3"] == [True, True, True, False, False]    # 默认：3 条
    # 判定记录钉住了各自生效的配置版本与快照
    d2_reject = _decisions(gov.audit, "d2")[1]
    assert d2_reject.reason is RejectReason.QUOTA_EXCEEDED
    assert d2_reject.config_version == 2
    assert d2_reject.config.max_reports_per_window == 1


def test_hot_update_takes_effect_immediately_for_target_device_only(gov):
    """热更新前后同一设备的判定对照；未覆盖的设备判定不变。"""
    assert gov.gw.connect("d1") is None
    assert gov.gw.connect("d2") is None
    for seq in range(3):  # 默认 3 条/窗
        assert gov.gw.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                                 received_at_ms=BASE + seq)).accepted
    blocked = gov.gw.ingest(env("http-json", http_payload("d1", 3, ts_ms=BASE + 3),
                                received_at_ms=BASE + 3))
    log_result(blocked)
    assert blocked.rejection.reason is RejectReason.QUOTA_EXCEEDED  # 旧配置：第 4 条被拒

    # 热更新：只放宽 d1，无需重启
    gov.configs.apply(ConfigUpdate(version=2, devices={
        "d1": DeviceConfig(2, 10, 1000)}))
    logger.info("hot update applied: version=%d devices=%s",
                gov.configs.version, list(gov.configs.snapshot_for_version(2).devices))

    after = [gov.gw.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                               received_at_ms=BASE + seq)) for seq in (4, 5, 6)]
    for r in after:
        log_result(r)
    assert all(r.accepted for r in after)  # 新配置立即生效

    # d2 仍按默认 3 条/窗
    for seq in range(3):
        assert gov.gw.ingest(env("http-json", http_payload("d2", seq, ts_ms=BASE + seq),
                                 received_at_ms=BASE + seq)).accepted
    d2_blocked = gov.gw.ingest(env("http-json", http_payload("d2", 3, ts_ms=BASE + 3),
                                   received_at_ms=BASE + 3))
    assert d2_blocked.rejection.reason is RejectReason.QUOTA_EXCEEDED

    # 追溯：d1 前 4 条判定钉 v1，后 3 条钉 v2
    versions = [e.config_version for e in _decisions(gov.audit, "d1")]
    logger.info("d1 decision config versions: %s", versions)
    assert versions == [1, 1, 1, 1, 2, 2, 2]


def test_replay_reproduces_historical_decisions(gov):
    """混合场景（接受/重复/配额拒绝/序列回退/跨配置版本）重放必须完全复现。"""
    assert gov.gw.connect("d1") is None
    gov.downstream.set_up(False)  # 制造暂存，覆盖补发路径
    gov.gw.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE), received_at_ms=BASE))
    gov.gw.ingest(env("http-json", http_payload("d1", 1, ts_ms=BASE + 1), received_at_ms=BASE + 1))
    # 完全重复投递 -> duplicate
    gov.gw.ingest(env("http-json", http_payload("d1", 1, ts_ms=BASE + 1), received_at_ms=BASE + 2))
    # 序列回退 -> rejected
    gov.gw.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE + 3, value=9.9),
                      received_at_ms=BASE + 3))
    # 窗口内第 3 条新数据收下，第 4 条超配额
    gov.gw.ingest(env("http-json", http_payload("d1", 2, ts_ms=BASE + 4), received_at_ms=BASE + 4))
    gov.gw.ingest(env("http-json", http_payload("d1", 3, ts_ms=BASE + 5), received_at_ms=BASE + 5))
    # 热更新放宽 -> 后续判定按 v2
    gov.configs.apply(ConfigUpdate(version=2, devices={"d1": DeviceConfig(2, 10, 1000)}))
    gov.gw.ingest(env("http-json", http_payload("d1", 4, ts_ms=BASE + 6), received_at_ms=BASE + 6))
    gov.gw.disconnect("d1")
    # 断连后上报 -> session_conflict
    gov.gw.ingest(env("http-json", http_payload("d1", 5, ts_ms=BASE + 7), received_at_ms=BASE + 7))

    entries = gov.audit.entries()
    for e in entries:
        logger.info("audit #%d %s device=%s decision=%s reason=%s config_v=%d",
                    e.seq_no, e.kind.value, e.device_id,
                    e.decision.value if e.decision else None,
                    e.reason.value if e.reason else None, e.config_version)
    mismatches = replay(entries)
    for m in mismatches:
        logger.error("mismatch at #%d: expected %s/%s got %s/%s", m.entry.seq_no,
                     m.entry.decision, m.entry.reason,
                     m.actual_decision, m.actual_reason)
    assert mismatches == []


def test_buffered_data_keeps_acceptance_time_semantics_after_config_change(gov):
    """已收下未转发的数据：改配置后补发不按新配置重判、顺序不变、结论不变。"""
    assert gov.gw.connect("d1") is None
    gov.downstream.set_up(False)
    for seq in range(3):  # v1 默认 3 条/窗，全部受理并入缓存
        assert gov.gw.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                                 received_at_ms=BASE + seq)).accepted
    assert len(gov.gw.buffer) == 3

    # 收紧到 1 条/窗：只影响新上报，不回溯已受理数据
    gov.configs.apply(ConfigUpdate(version=2, devices={"d1": DeviceConfig(2, 1, 1000)}))
    gov.downstream.set_up(True)
    sent = gov.gw.flush()
    stream = [(r.offset, r.point.seq) for r in gov.downstream.received]
    logger.info("replay-after-config-change stream: %s", stream)
    assert sent == 3
    assert [s for _, s in stream] == [0, 1, 2]  # 全部补发，顺序不变
    # 三条判定记录钉的都是受理时的 v1
    assert [e.config_version for e in _decisions(gov.audit, "d1")] == [1, 1, 1]
    # 新上报按 v2：窗口重算后只收 1 条
    assert gov.gw.ingest(env("http-json", http_payload("d1", 3, ts_ms=BASE + 10),
                             received_at_ms=BASE + 10)).accepted
    blocked = gov.gw.ingest(env("http-json", http_payload("d1", 4, ts_ms=BASE + 11),
                                  received_at_ms=BASE + 11))
    assert blocked.rejection.reason is RejectReason.QUOTA_EXCEEDED
    assert replay(gov.audit.entries()) == []


def test_config_switch_concurrent_with_ingest_and_replay_is_deterministic(gov):
    """配置热更新与并发上报/补发同时进行：无异常、无重复判定、无丢失、顺序不乱，
    且全部历史记录重放复现。"""
    errors: list[BaseException] = []
    accepted: list[bool] = []
    lock = threading.Lock()

    for dev in ("d1", "d2"):
        assert gov.gw.connect(dev) is None

    def producer(dev: str) -> None:
        try:
            for i in range(30):
                r = gov.gw.ingest(env("http-json",
                                      http_payload(dev, i, ts_ms=BASE + i),
                                      received_at_ms=BASE + i))
                with lock:
                    accepted.append(r.accepted and not r.duplicate)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    def chaos() -> None:
        try:
            for v in range(2, 8):  # 合法且版本单调前进的热更新
                gov.configs.apply(ConfigUpdate(version=v, devices={
                    "d1": DeviceConfig(2, 3 + v, 1000)}))
                gov.downstream.set_up(v % 2 == 0)  # 交替制造故障/恢复
                gov.gw.flush()
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = ([threading.Thread(target=producer, args=(d,)) for d in ("d1", "d2")]
               + [threading.Thread(target=chaos)])
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    gov.downstream.set_up(True)
    gov.gw.flush()

    assert errors == []
    # 无丢失：受理条数 == 已转发条数（缓存已排空）
    assert len(gov.gw.buffer) == 0
    n_accepted = sum(accepted)
    assert len(gov.downstream.received) == n_accepted
    # 补发流水：offset 唯一且严格升序（无重复判定、顺序未被打乱）
    offsets = [r.offset for r in gov.downstream.received]
    logger.info("concurrent replay offsets: %s", offsets)
    assert offsets == sorted(offsets) == list(dict.fromkeys(offsets))
    # 每条判定钉的配置版本都属于实际生效过的版本
    versions = {e.config_version for e in gov.audit.entries()}
    logger.info("config versions seen in audit: %s", sorted(versions))
    assert versions <= set(range(1, 8))
    # 全量重放：结论必须与当时完全一致
    mismatches = replay(gov.audit.entries())
    assert mismatches == []


def test_invalid_config_batch_rejected_atomically(gov):
    """非法配置或版本倒退：整批拒绝，原生效配置（含其他设备的覆盖）不变。"""
    gov.configs.apply(ConfigUpdate(version=2, devices={"d1": DeviceConfig(2, 5, 1000)}))
    before = gov.configs.snapshot_for_version(gov.configs.version)

    # 版本不前进
    with pytest.raises(ConfigError):
        gov.configs.apply(ConfigUpdate(version=2, devices={"d2": DeviceConfig(1, 1, 1)}))
    # 批量中任一设备非法 -> 整批拒绝，同批的合法设备也不生效
    with pytest.raises(ConfigError):
        gov.configs.apply(ConfigUpdate(version=3, devices={
            "d2": DeviceConfig(1, 1, 1),       # 合法
            "d3": DeviceConfig(0, 1, 1000),    # 非法：并发上限为 0
        }))
    with pytest.raises(ConfigError):
        gov.configs.apply(ConfigUpdate(version=3,
                                       default=DeviceConfig(1, -1, 1000)))  # 非法默认配置

    after = gov.configs.snapshot_for_version(gov.configs.version)
    logger.info("config unchanged after rejected batches: v=%d devices=%s",
                after.version, sorted(after.devices))
    assert gov.configs.version == 2
    assert after == before
    assert gov.configs.effective_for("d2").config == DEFAULT_CFG  # 同批合法项也未生效
    assert gov.configs.effective_for("d1").config.max_reports_per_window == 5


def test_audit_log_is_bounded(gov):
    """追溯日志容量有界：超出淘汰最旧并计数，查询仍可用。"""
    small = AuditLog(capacity=5)
    normalizer = Normalizer()
    normalizer.register(HttpJsonAdapter())
    sessions = SessionManager(QuotaPolicy(version=1, max_concurrent_sessions_per_device=2,
                                          max_reports_per_window=100, window_ms=1000))
    configs = ConfigRegistry(DeviceConfig(2, 100, 1000), version=1)
    gw = Gateway(normalizer, sessions, SpoolBuffer(16), SimulatedDownstream(),
                 config_registry=configs, audit_log=small)
    gw.connect("d1")
    for seq in range(12):
        gw.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                      received_at_ms=BASE + seq))
    logger.info("audit bounded: len=%d dropped=%d capacity=%d",
                len(small), small.dropped, small.capacity)
    assert len(small) == 5
    assert small.dropped == 8  # 1 次 connect + 12 条判定 - 5 容量
    rest = small.query(device_id="d1")
    assert [e.seq_no for e in rest] == sorted(e.seq_no for e in rest)
    # 容量淘汰后剩余的是历史残片（connect 事件可能已被淘汰），不保证可完整重放；
    # 这里只验证对残片执行重放不会崩溃，全量记录的可重放性由专门用例保证。
    replay(rest)


def test_audit_query_by_device_and_time_range(gov):
    """按设备 / 按时间段查询判定记录。"""
    assert gov.gw.connect("d1") is None
    assert gov.gw.connect("d2") is None
    for seq in range(3):
        gov.gw.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                          received_at_ms=BASE + seq))
    gov.gw.ingest(env("http-json", http_payload("d2", 0, ts_ms=BASE + 100),
                      received_at_ms=BASE + 100))

    d1 = _decisions(gov.audit, "d1")
    d2 = _decisions(gov.audit, "d2")
    assert len(d1) == 3 and len(d2) == 1
    ranged = gov.audit.query(start_ms=BASE + 1, end_ms=BASE + 2)
    logger.info("time-range query [%d,%d] -> seq_no=%s", BASE + 1, BASE + 2,
                [e.seq_no for e in ranged])
    assert [e.received_at_ms for e in ranged] == [BASE + 1, BASE + 2]
    assert all(BASE + 1 <= e.received_at_ms <= BASE + 2 for e in ranged)


def test_http_config_hot_update_and_audit_query():
    """HTTP 全链路：不重启热更新配置 -> 新判定立即生效 -> 判定记录可查询。"""
    import importlib
    import time

    from fastapi.testclient import TestClient

    import main
    importlib.reload(main)  # 保证全局网关状态独立
    client = TestClient(main.app)

    now = int(time.time() * 1000)
    assert client.post("/connect/d1").json()["ok"] is True

    # 默认 100 条/窗收紧为 d1 专用 1 条/窗（版本必须前进）
    resp = client.post("/config", json={
        "version": 2,
        "devices": {"d1": {"max_concurrent_sessions_per_device": 1,
                           "max_reports_per_window": 1, "window_ms": 60_000}},
    })
    assert resp.json()["ok"] is True
    assert resp.json()["config"]["version"] == 2

    payload = {"device_id": "d1", "metric": "temp", "value": 1.0, "ts_ms": now, "seq": 0}
    assert client.post("/ingest/http-json", json=payload).json()["accepted"] is True
    payload["seq"] = 1
    payload["ts_ms"] = now + 1
    blocked = client.post("/ingest/http-json", json=payload).json()
    logger.info("after hot update, second report: %s", blocked)
    assert blocked["accepted"] is False and blocked["reason"] == "quota_exceeded"

    # 非法/版本倒退：整批拒绝，原配置不变
    bad = client.post("/config", json={"version": 2, "devices": {}})
    assert bad.status_code == 409
    invalid = client.post("/config", json={
        "version": 3,
        "devices": {"d1": {"max_concurrent_sessions_per_device": 0,
                           "max_reports_per_window": 1, "window_ms": 60_000}},
    })
    assert invalid.status_code == 409
    assert client.get("/config").json()["version"] == 2

    # 判定追溯：按设备查询，记录钉住配置版本
    audit = client.get("/audit", params={"device_id": "d1"}).json()
    logger.info("audit entries for d1: %s", audit["entries"])
    decisions = [e for e in audit["entries"] if e["kind"] == "decision"]
    assert [e["decision"] for e in decisions] == ["accepted", "rejected"]
    assert all(e["config_version"] == 2 for e in decisions)
    assert audit["capacity"] > 0 and audit["dropped"] == 0
