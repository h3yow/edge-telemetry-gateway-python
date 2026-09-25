"""测试公共夹具：构建全新网关实例，日志打印输入/判定依据/补发流水。"""
from __future__ import annotations

import logging

import pytest

from gateway.buffer import SpoolBuffer
from gateway.downstream import SimulatedDownstream
from gateway.gateway import Gateway
from gateway.models import RawEnvelope
from gateway.normalizer import HttpJsonAdapter, ModbusAdapter, Normalizer
from gateway.session import QuotaPolicy, SessionManager

logger = logging.getLogger("gateway.test")

NOW_MS = 1_800_000_000_000  # 固定基准时刻，测试可复现


@pytest.fixture(autouse=True)
def configure_logging(caplog):
    caplog.set_level(logging.DEBUG)
    yield


@pytest.fixture
def downstream() -> SimulatedDownstream:
    return SimulatedDownstream()


@pytest.fixture
def sessions() -> SessionManager:
    return SessionManager(QuotaPolicy(
        version=1,
        max_concurrent_sessions_per_device=1,
        max_reports_per_window=3,
        window_ms=1000,
    ))


@pytest.fixture
def gateway(sessions, downstream) -> Gateway:
    normalizer = Normalizer()
    normalizer.register(HttpJsonAdapter())
    normalizer.register(ModbusAdapter())
    return Gateway(normalizer, sessions, SpoolBuffer(capacity=4), downstream)


def env(source: str, payload: dict, received_at_ms: int = NOW_MS) -> RawEnvelope:
    e = RawEnvelope(source=source, payload=payload, received_at_ms=received_at_ms)
    logger.info("input envelope: source=%s payload=%s received_at_ms=%d",
                e.source, e.payload, e.received_at_ms)
    return e


def http_payload(device: str, seq: int, metric: str = "temp", value: float = 1.0,
                 ts_ms: int = NOW_MS) -> dict:
    return {"device_id": device, "metric": metric, "value": value,
            "ts_ms": ts_ms, "seq": seq}


def log_result(result) -> None:
    if result.accepted:
        logger.info("decision: accepted duplicate=%s point=%s", result.duplicate, result.point)
    else:
        logger.info("decision: rejected reason=%s detail=%s origin=%s",
                    result.rejection.reason.value, result.rejection.detail,
                    result.rejection.envelope.payload)
