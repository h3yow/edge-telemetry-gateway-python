"""本地有界缓存与按序补发。

确定性规则：
- 容量有界：满时 store 返回 FULL，由上层拒绝（RejectReason.BUFFER_FULL），
  绝不静默丢弃，内存不会无上限增长；
- 去重键 = (device_id, metric, seq)：重复投递返回 DUPLICATE（幂等，不重复入缓存）；
  已确认（ack）的键保留在有界最近确认集中，重试历史数据同样识别为 DUPLICATE；
- 每条记录分配单调递增 offset，补发严格按 offset 升序；
- ack_through 确认 <= offset 的记录并截断，确认集大小有界（= capacity）。
"""
from __future__ import annotations

import enum
from collections import deque
from dataclasses import dataclass

from .models import TelemetryPoint


class StoreResult(enum.Enum):
    STORED = "stored"            # 新数据入缓存
    DUPLICATE = "duplicate"      # 重复投递，幂等忽略
    FULL = "full"                # 暂存空间耗尽


@dataclass(frozen=True)
class BufferedRecord:
    """缓存记录：单调 offset 保证补发顺序确定。"""

    offset: int
    point: TelemetryPoint


def _key(point: TelemetryPoint) -> tuple[str, str, int]:
    return (point.device_id, point.metric, point.seq)


class SpoolBuffer:
    """有界暂存：容量上限、去重、按 offset 顺序补发、确认后截断。"""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._records: list[BufferedRecord] = []
        self._pending_keys: set[tuple[str, str, int]] = set()
        self._acked_keys: set[tuple[str, str, int]] = set()
        self._acked_order: deque[tuple[str, str, int]] = deque()
        self._next_offset = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._records)

    def store(self, point: TelemetryPoint) -> StoreResult:
        k = _key(point)
        if k in self._pending_keys or k in self._acked_keys:
            return StoreResult.DUPLICATE
        if len(self._records) >= self._capacity:
            return StoreResult.FULL
        rec = BufferedRecord(offset=self._next_offset, point=point)
        self._next_offset += 1
        self._records.append(rec)
        self._pending_keys.add(k)
        return StoreResult.STORED

    def pending(self, limit: int | None = None) -> list[BufferedRecord]:
        """按 offset 升序返回待补发记录。"""
        recs = sorted(self._records, key=lambda r: r.offset)
        return recs if limit is None else recs[:limit]

    def ack_through(self, offset: int) -> int:
        """确认 <= offset 的记录并移除，返回移除条数。"""
        kept: list[BufferedRecord] = []
        removed = 0
        for rec in self._records:
            if rec.offset <= offset:
                removed += 1
                k = _key(rec.point)
                self._pending_keys.discard(k)
                self._remember_acked(k)
            else:
                kept.append(rec)
        self._records = kept
        return removed

    def _remember_acked(self, k: tuple[str, str, int]) -> None:
        """有界最近确认集：超出 capacity 时淘汰最旧键，内存有界。"""
        if k in self._acked_keys:
            return
        self._acked_keys.add(k)
        self._acked_order.append(k)
        while len(self._acked_order) > self._capacity:
            old = self._acked_order.popleft()
            self._acked_keys.discard(old)
