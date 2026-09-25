"""测试公共装置：日志助手与网关工厂。

日志约定（单测中打印，便于人工核对）：
- [输入]   送入网关的原始报文与上下文
- [判定]   网关返回的接受/拒绝及依据
- [补发]   下游恢复时的补发流水
"""

from __future__ import annotations

import pytest

from gateway import Gateway, QuotaConfig
from gateway.models import ForwardResult, IngestResult

NOW = 1_700_000_000_000  # 固定基准时间，保证测试可复现


def log_input(raw: str, protocol: str, device: str, session: str, now_ms: int) -> None:
    print(f"\n[输入] protocol={protocol} device={device} session={session} "
          f"now={now_ms} raw={raw!r}")


def log_decision(result: IngestResult) -> None:
    if result.accepted:
        print(f"[判定] 接受 detail={result.detail} "
              f"config_version={result.record.config_version if result.record else '-'}")
    else:
        print(f"[判定] 拒绝 reason={result.reason.value} detail={result.detail} "
              f"出处 protocol={result.source_protocol} raw={result.raw!r}")


def log_replay(flow: list[ForwardResult]) -> None:
    print(f"[补发] 共 {len(flow)} 条流水:")
    for f in flow:
        print(f"  attempt={f.attempt} device={f.record.device_id} "
              f"seq={f.record.seq} delivered={f.delivered} note={f.note}")


@pytest.fixture
def make_gateway():
    def _make(quota: QuotaConfig | None = None, capacity: int = 100) -> Gateway:
        return Gateway(quota or QuotaConfig(), buffer_capacity=capacity)
    return _make
