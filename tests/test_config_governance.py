"""配置治理与判定可追溯。

验证：
- 按设备差异化配置：不同设备不同的并发/频率上限，互不影响；
- 运行期热更新：同一设备更新前后判定对照，判定记录带当时配置版本；
- 历史判定可按设备/时间段查询，重放得到与当时一模一样的结论；
- 配置切换与并发上报/补发同时进行时的确定性（版本单调、不丢不重、顺序不乱）；
- 非法配置/版本不前进整批拒绝，原生效配置不变；
- 已受理未转发的数据补发时仍按受理时的配置版本入账；
- 台账容量有界，超出淘汰最旧。
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

import pytest

from gateway.audit import Decision, DecisionLedger, RecordKind, replay
from gateway.buffer import SpoolBuffer
from gateway.config import ConfigRegistry, DeviceConfig
from gateway.downstream import SimulatedDownstream
from gateway.gateway import Gateway
from gateway.models import RejectReason
from gateway.normalizer import HttpJsonAdapter, ModbusAdapter, Normalizer
from gateway.session import SessionManager
from tests.conftest import env, http_payload, log_result

logger = logging.getLogger("gateway.test")
BASE = 1_800_000_000_000


@dataclass
class Rig:
    gateway: Gateway
    registry: ConfigRegistry
    ledger: DecisionLedger
    downstream: SimulatedDownstream
    normalizer: Normalizer


@pytest.fixture
def rig() -> Rig:
    registry = ConfigRegistry(DeviceConfig(
        device_id=None, version=1, max_concurrent_sessions=1,
        max_reports_per_window=3, window_ms=1000))
    normalizer = Normalizer()
    normalizer.register(HttpJsonAdapter())
    normalizer.register(ModbusAdapter())
    ledger = DecisionLedger(capacity=500)
    downstream = SimulatedDownstream()
    gateway = Gateway(normalizer, SessionManager(registry=registry),
                      SpoolBuffer(capacity=128), downstream, ledger)
    return Rig(gateway, registry, ledger, downstream, normalizer)


def _versions(rig: Rig, device: str) -> list[int]:
    return [r.config_version for r in rig.ledger.query(device_id=device)
            if r.kind is RecordKind.DECISION]


def test_per_device_differential_config(rig: Rig):
    """d1 覆盖窗口配额 2、并发 2；d2 覆盖窗口配额 4；其余设备用默认 3。"""
    assert rig.registry.apply_updates([
        DeviceConfig("d1", 1, max_concurrent_sessions=2,
                     max_reports_per_window=2, window_ms=1000),
        DeviceConfig("d2", 1, max_concurrent_sessions=1,
                     max_reports_per_window=4, window_ms=1000),
    ]).applied
    # 并发上限差异化：d1 可开 2 个连接，d2 只能 1 个
    assert rig.gateway.connect("d1", BASE) is None
    assert rig.gateway.connect("d1", BASE) is None
    assert rig.gateway.connect("d1", BASE) is RejectReason.QUOTA_EXCEEDED
    assert rig.gateway.connect("d2", BASE) is None
    assert rig.gateway.connect("d2", BASE) is RejectReason.QUOTA_EXCEEDED
    assert rig.gateway.connect("d3", BASE) is None  # 默认配置设备照常接入

    results = {}
    for dev, n in [("d1", 3), ("d2", 5), ("d3", 4)]:
        rs = [rig.gateway.ingest(env("http-json", http_payload(dev, seq, ts_ms=BASE + seq),
                                     received_at_ms=BASE + seq))
              for seq in range(n)]
        for r in rs:
            log_result(r)
        results[dev] = rs
    # d1 配额 2：第 3 条拒；d2 配额 4：第 5 条拒；d3 默认 3：第 4 条拒
    assert [r.accepted for r in results["d1"]] == [True, True, False]
    assert [r.accepted for r in results["d2"]] == [True, True, True, True, False]
    assert [r.accepted for r in results["d3"]] == [True, True, True, False]
    assert results["d1"][2].rejection.reason is RejectReason.QUOTA_EXCEEDED
    # 判定记录携带各自配置版本（d1/d2 覆盖链 v1，d3 默认链 v1）
    assert _versions(rig, "d1") == [1, 1, 1]
    assert _versions(rig, "d2") == [1] * 5
    logger.info("per-device decisions: d1=%s d2=%s d3=%s",
                _versions(rig, "d1"), _versions(rig, "d2"), _versions(rig, "d3"))


def test_hot_update_changes_only_target_device(rig: Rig):
    """热更新前后同一设备判定对照；更新只影响目标设备。"""
    rig.gateway.connect("d1", BASE)
    rig.gateway.connect("d2", BASE)
    for seq in range(3):  # 默认配额 3，占满窗口
        assert rig.gateway.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                                      received_at_ms=BASE + seq)).accepted
        assert rig.gateway.ingest(env("http-json", http_payload("d2", seq, ts_ms=BASE + seq),
                                      received_at_ms=BASE + seq)).accepted
    blocked = rig.gateway.ingest(env("http-json", http_payload("d1", 3, ts_ms=BASE + 3),
                                     received_at_ms=BASE + 3))
    log_result(blocked)
    assert blocked.rejection.reason is RejectReason.QUOTA_EXCEEDED  # 更新前：拒

    # 热更新 d1 -> v2 放宽配额；不重启、不影响 d2
    res = rig.registry.apply_updates([
        DeviceConfig("d1", 2, 1, max_reports_per_window=10, window_ms=1000,
                     applied_at_ms=BASE + 4)])
    assert res.applied
    after = rig.gateway.ingest(env("http-json", http_payload("d1", 4, ts_ms=BASE + 4),
                                   received_at_ms=BASE + 4))
    log_result(after)
    assert after.accepted  # 更新后：同设备同类上报被接受
    # d2 仍按默认配置：窗口已满，继续上报被拒
    d2_blocked = rig.gateway.ingest(env("http-json", http_payload("d2", 3, ts_ms=BASE + 5),
                                        received_at_ms=BASE + 5))
    assert d2_blocked.rejection.reason is RejectReason.QUOTA_EXCEEDED
    # 判定记录：d1 前 4 条 v1、更新后 v2；d2 全程默认链 v1
    assert _versions(rig, "d1") == [1, 1, 1, 1, 2]
    assert _versions(rig, "d2") == [1, 1, 1, 1]
    logger.info("hot update before/after: d1 versions=%s", _versions(rig, "d1"))


def test_replay_reproduces_historical_decisions(rig: Rig):
    """跨版本、含重复与拒绝的历史记录，重放结论与当时一致。"""
    rig.gateway.connect("d1", BASE)
    rig.gateway.ingest(env("http-json", http_payload("d1", 0, ts_ms=BASE), received_at_ms=BASE))
    rig.gateway.ingest(env("http-json", http_payload("d1", 1, ts_ms=BASE + 1),
                           received_at_ms=BASE + 1))
    # 重复投递（幂等）
    dup = rig.gateway.ingest(env("http-json", http_payload("d1", 1, ts_ms=BASE + 1),
                                 received_at_ms=BASE + 2))
    assert dup.duplicate
    # 窗口配额 3：第 3 条新数据占满，第 4 条拒
    rig.gateway.ingest(env("http-json", http_payload("d1", 2, ts_ms=BASE + 3),
                           received_at_ms=BASE + 3))
    rejected = rig.gateway.ingest(env("http-json", http_payload("d1", 3, ts_ms=BASE + 4),
                                      received_at_ms=BASE + 4))
    assert rejected.rejection.reason is RejectReason.QUOTA_EXCEEDED
    # 热更新到 v2，序列回退判定在新版本下依旧
    rig.registry.apply_updates([DeviceConfig("d1", 2, 1, 5, 1000, applied_at_ms=BASE + 5)])
    regress = rig.gateway.ingest(env("http-json", http_payload("d1", 1, ts_ms=BASE + 5, value=9.0),
                                     received_at_ms=BASE + 5))
    assert regress.rejection.reason is RejectReason.SEQUENCE_REGRESSION
    ok = rig.gateway.ingest(env("http-json", http_payload("d1", 4, ts_ms=BASE + 6),
                                received_at_ms=BASE + 6))
    assert ok.accepted

    records = rig.ledger.query(device_id="d1")
    logger.info("history: %s", [(r.seq_no, r.kind.value, r.decision.value,
                                 r.reason, r.config_version) for r in records])
    outcomes = replay(records, rig.registry, rig.normalizer)
    mismatches = [o for o in outcomes if not o.matches]
    assert not mismatches, mismatches
    # 重放覆盖全部非补发记录，且版本分布与当时一致
    assert len(outcomes) == len([r for r in records if r.kind is not RecordKind.FORWARD])
    logger.info("replay matched %d records", len(outcomes))


def test_query_by_device_and_time_range(rig: Rig):
    """判定记录可按设备与时间段查询。"""
    rig.gateway.connect("d1", BASE)
    rig.gateway.connect("d2", BASE)
    for seq in range(2):
        rig.gateway.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                               received_at_ms=BASE + seq))
        rig.gateway.ingest(env("http-json", http_payload("d2", seq, ts_ms=BASE + 100 + seq),
                               received_at_ms=BASE + 100 + seq))
    d1_all = rig.ledger.query(device_id="d1", kinds=[RecordKind.DECISION])
    assert len(d1_all) == 2 and all(r.device_id == "d1" for r in d1_all)
    windowed = rig.ledger.query(since_ms=BASE + 100, kinds=[RecordKind.DECISION])
    assert windowed and all(r.received_at_ms >= BASE + 100 for r in windowed)
    assert {r.device_id for r in windowed} == {"d2"}
    seqs = [r.seq_no for r in rig.ledger.query()]
    assert seqs == sorted(seqs)  # 全局顺序
    logger.info("query d1=%d records, since+100 -> devices=%s",
                len(d1_all), {r.device_id for r in windowed})


def test_invalid_or_regressing_batch_rejected_atomically(rig: Rig):
    """非法字段/版本不前进：整批拒绝，已生效配置不变。"""
    before_d1 = rig.registry.effective_for("d1")
    before_default = rig.registry.effective_for("d3")
    bad = rig.registry.apply_updates([
        DeviceConfig("d1", 1, 1, 5, 1000),          # 本身合法
        DeviceConfig("d2", 1, 0, 5, 1000),          # 非法并发上限
    ])
    assert not bad.applied and bad.errors
    logger.info("rejected batch errors: %s", bad.errors)
    assert rig.registry.effective_for("d1") == before_d1  # d1 未被部分生效
    assert rig.registry.effective_for("d2").device_id is None

    ok = rig.registry.apply_updates([DeviceConfig("d1", 1, 1, 5, 1000)])
    assert ok.applied
    # 版本不前进（持平/回退）整批拒绝
    for v in (1, 0):
        res = rig.registry.apply_updates([DeviceConfig("d1", v, 1, 6, 1000)])
        assert not res.applied
        assert rig.registry.effective_for("d1").max_reports_per_window == 5
    # 默认配置版本同样受单调约束
    res = rig.registry.apply_updates([DeviceConfig(None, 1, 1, 3, 1000)])
    assert not res.applied
    assert rig.registry.effective_for("d3") == before_default


def test_buffered_data_replayed_with_acceptance_version(rig: Rig):
    """已受理未转发的数据，补发流水记的是受理时的配置版本。"""
    rig.gateway.connect("d1", BASE)
    rig.downstream.set_up(False)
    for seq in range(2):
        assert rig.gateway.ingest(env("http-json", http_payload("d1", seq, ts_ms=BASE + seq),
                                      received_at_ms=BASE + seq)).accepted
    # 故障期间改配置：已受理数据语义不变
    rig.registry.apply_updates([DeviceConfig("d1", 2, 1, 1, 1000, applied_at_ms=BASE + 2)])
    rig.downstream.set_up(True)
    sent = rig.gateway.flush()
    assert sent == 2
    forwards = rig.ledger.query(device_id="d1", kinds=[RecordKind.FORWARD])
    logger.info("forward records: %s", [(f.detail, f.config_version) for f in forwards])
    assert [f.config_version for f in forwards] == [1, 1]  # 受理时版本，非 v2
    # 补发顺序不变
    assert [r.point.seq for r in rig.downstream.received] == [0, 1]


def test_ledger_capacity_is_bounded(rig: Rig):
    """台账容量有界：超出淘汰最旧，seq_no 连续。"""
    small = DecisionLedger(capacity=5)
    gw = Gateway(rig.normalizer, SessionManager(registry=rig.registry),
                 SpoolBuffer(16), SimulatedDownstream(), small)
    gw.connect("d9", BASE)
    for seq in range(9):
        gw.ingest(env("http-json", http_payload("d9", seq, ts_ms=BASE + seq * 2000),
                      received_at_ms=BASE + seq * 2000))
    assert len(small) <= 5
    recs = small.query()
    assert recs[0].seq_no == small.query()[-1].seq_no - len(recs) + 1
    logger.info("bounded ledger: kept seq_no %s", [r.seq_no for r in recs])


def test_config_switch_concurrent_with_ingest_and_flush_is_deterministic(rig: Rig):
    """配置切换与并发上报/补发同时进行：版本单调、不丢不重、补发有序。"""
    n_reports = 60
    rig.registry.apply_updates([DeviceConfig("d1", 1, 1, 1000, 10_000_000)])
    rig.gateway.connect("d1", BASE)
    rig.downstream.set_up(False)  # 数据留在缓存，补发与切换并发
    errors: list[Exception] = []

    def reporter() -> None:
        try:
            for seq in range(n_reports):
                r = rig.gateway.ingest(env("http-json",
                                           http_payload("d1", seq, ts_ms=BASE + seq),
                                           received_at_ms=BASE + seq))
                assert r.accepted  # 配额足够大，不应拒绝
        except Exception as e:  # pragma: no cover
            errors.append(e)

    def updater() -> None:
        try:
            for v in range(2, 8):  # 反复热更新，与上报并发
                res = rig.registry.apply_updates(
                    [DeviceConfig("d1", v, 1, 1000 + v, 10_000_000,
                                  applied_at_ms=BASE + v)])
                assert res.applied
        except Exception as e:  # pragma: no cover
            errors.append(e)

    def flusher() -> None:
        try:
            for _ in range(n_reports // 4):
                rig.gateway.flush()
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=reporter),
               threading.Thread(target=updater),
               threading.Thread(target=flusher)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    rig.downstream.set_up(True)
    rig.gateway.flush()
    decisions = [r for r in rig.ledger.query(device_id="d1")
                 if r.kind is RecordKind.DECISION]
    logger.info("concurrent run: %d decisions, versions=%s",
                len(decisions), [r.config_version for r in decisions])
    # 每条上报恰好判定一次，结论唯一
    assert len(decisions) == n_reports
    assert all(r.decision is Decision.ACCEPTED for r in decisions)
    # 判定版本单调不降：不存在新旧配置交错判定同一批数据
    versions = [r.config_version for r in decisions]
    assert versions == sorted(versions)
    assert set(versions) <= set(range(1, 8))
    # 补发流水：offset 严格递增、无重复、无丢失
    offsets = [r.offset for r in rig.downstream.received]
    assert offsets == sorted(offsets) and len(set(offsets)) == n_reports
    forwards = rig.ledger.query(device_id="d1", kinds=[RecordKind.FORWARD])
    assert len(forwards) == n_reports
    logger.info("replay stream offsets ok, %d forwards recorded", len(forwards))
