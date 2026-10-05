"""判定可追溯：有界判定台账与确定性重放。

- 每条上报的判定（接受/重复/拒绝）、会话开/关、转发补发，都连同当时生效的
  配置版本一起入台账；记录携带原始 RawEnvelope，出处完整；
- 台账容量有界：超出淘汰最旧记录，内存不会无限增长；
- 可按设备 / 时间段 / 记录类型查询，按 seq_no（全局接收顺序）升序返回；
- 重放（replay）：用记录中的配置版本从 ConfigRegistry 的版本历史取回当时配置，
  以冻结视角重建 SessionManager，配合相同的归一化器与不受限缓存重放判定，
  结论（判定 + 原因）与当时一致——重放复现的是配置敏感判定；BUFFER_FULL 由
  下游瞬时可用性驱动、不属于配置语义，重放时缓存容量不受限，该记录不参与比较。
"""
from __future__ import annotations

import enum
import logging
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass

from .config import ConfigRegistry, DeviceConfig
from .models import RawEnvelope
from .normalizer import Normalizer
from .session import SessionManager

logger = logging.getLogger("gateway.audit")


class RecordKind(enum.Enum):
    """台账记录类型。"""

    DECISION = "decision"            # 上报判定（接受/重复/拒绝）
    SESSION_OPEN = "session_open"    # 建立连接（含配额拒绝）
    SESSION_CLOSE = "session_close"  # 关闭连接
    FORWARD = "forward"              # 转发/补发流水（携带受理时的配置版本）


class Decision(enum.Enum):
    """单条报文/事件的最终判定。"""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


@dataclass(frozen=True)
class DecisionRecord:
    """一条台账记录：输入、结论、判定依据（当时生效的配置版本）。"""

    seq_no: int                 # 台账内单调序号（全局事件顺序）
    kind: RecordKind
    device_id: str
    decision: Decision
    reason: str | None          # 拒绝原因（RejectReason.value），非拒绝为 None
    config_version: int         # 事件发生时该设备生效的配置版本
    received_at_ms: int
    envelope: RawEnvelope       # 原始报文（会话事件为空 payload 占位），重放的输入
    detail: str = ""


class ReplayError(RuntimeError):
    """重放无法进行（如配置版本历史已淘汰）。"""


@dataclass(frozen=True)
class ReplayOutcome:
    """单条记录的重放结果与当时结论的对照。"""

    record: DecisionRecord
    decision: Decision
    reason: str | None

    @property
    def matches(self) -> bool:
        return self.decision == self.record.decision and self.reason == self.record.reason


class DecisionLedger:
    """有界判定台账：容量固定，超出淘汰最旧；查询按设备/时间段/类型过滤。"""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._records: deque[DecisionRecord] = deque()
        self._next_seq = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    def append(self, kind: RecordKind, device_id: str, decision: Decision,
               reason: str | None, config_version: int, received_at_ms: int,
               envelope: RawEnvelope, detail: str = "") -> DecisionRecord:
        """追加一条记录（seq_no 由台账分配，保证全局单调），返回完整记录。"""
        rec = DecisionRecord(
            seq_no=self._next_seq, kind=kind, device_id=device_id, decision=decision,
            reason=reason, config_version=config_version,
            received_at_ms=received_at_ms, envelope=envelope, detail=detail,
        )
        self._next_seq += 1
        self._records.append(rec)
        while len(self._records) > self._capacity:
            self._records.popleft()
        return rec

    def query(self, device_id: str | None = None,
              since_ms: int | None = None,
              until_ms: int | None = None,
              kinds: Iterable[RecordKind] | None = None) -> list[DecisionRecord]:
        """按设备/时间段（received_at_ms 闭区间）/类型过滤，按 seq_no 升序。"""
        kind_set = set(kinds) if kinds is not None else None
        out: list[DecisionRecord] = []
        for r in self._records:
            if device_id is not None and r.device_id != device_id:
                continue
            if since_ms is not None and r.received_at_ms < since_ms:
                continue
            if until_ms is not None and r.received_at_ms > until_ms:
                continue
            if kind_set is not None and r.kind not in kind_set:
                continue
            out.append(r)
        return out

    def __len__(self) -> int:
        return len(self._records)


def replay(records: list[DecisionRecord], registry: ConfigRegistry,
           normalizer: Normalizer) -> list[ReplayOutcome]:
    """用记录中的配置版本重放判定，返回每条记录与当时结论的对照。

    - FORWARD 记录是补发流水，不参与判定重放（跳过）；
    - 其余记录按 seq_no 升序，用冻结配置视角（每条记录生效的版本）重建
      SessionManager，配合相同的归一化器与容量不受限的缓存重放；
    - 配置版本历史已淘汰时抛 ReplayError（无法忠实重放）。
    """
    from .buffer import SpoolBuffer        # 延迟导入，避免与 gateway 编排模块循环依赖
    from .downstream import SimulatedDownstream
    from .gateway import Gateway

    recs = sorted((r for r in records if r.kind is not RecordKind.FORWARD),
                  key=lambda r: r.seq_no)
    # 预取每条记录生效版本的配置；任一版本已淘汰则无法忠实重放
    configs: dict[tuple[str, int], DeviceConfig] = {}
    for r in recs:
        key = (r.device_id, r.config_version)
        if key not in configs:
            cfg = registry.config_at_version(r.device_id, r.config_version)
            if cfg is None:
                raise ReplayError(
                    f"config version {r.config_version} for device "
                    f"'{r.device_id}' has been evicted from history")
            configs[key] = cfg

    current_version: dict[str, int] = {}

    def frozen_provider(device_id: str) -> DeviceConfig:
        return configs[(device_id, current_version[device_id])]

    sessions = SessionManager(config_provider=frozen_provider)
    # 容量不受限：BUFFER_FULL 由下游瞬时可用性驱动，不属于配置语义
    buffer = SpoolBuffer(capacity=max(1, len(recs)))
    downstream = SimulatedDownstream()
    gateway = Gateway(normalizer, sessions, buffer, downstream)

    outcomes: list[ReplayOutcome] = []
    for r in recs:
        current_version[r.device_id] = r.config_version
        if r.kind is RecordKind.SESSION_OPEN:
            reason = sessions.open_connection(r.device_id)
            actual = (Decision.ACCEPTED, None) if reason is None \
                else (Decision.REJECTED, reason.value)
        elif r.kind is RecordKind.SESSION_CLOSE:
            sessions.close_connection(r.device_id)
            actual = (Decision.ACCEPTED, None)
        else:
            result = gateway.ingest(r.envelope)
            if result.accepted:
                actual = (Decision.DUPLICATE if result.duplicate else Decision.ACCEPTED, None)
            else:
                assert result.rejection is not None
                actual = (Decision.REJECTED, result.rejection.reason.value)
        outcome = ReplayOutcome(record=r, decision=actual[0], reason=actual[1])
        outcomes.append(outcome)
        logger.info("replay seq_no=%d device=%s expected=%s/%s actual=%s/%s match=%s",
                    r.seq_no, r.device_id, r.decision.value, r.reason,
                    outcome.decision.value, outcome.reason, outcome.matches)
    return outcomes
