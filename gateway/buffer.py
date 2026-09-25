"""本地有界暂存与按序补发。

确定规则：
- 容量有界：达到 capacity 后新记录入队被拒绝（BUFFER_FULL），内存不会无上限增长；
- 去重：以 (device_id, seq) 为键，同一键重复入队被识别为 DUPLICATE 并拒绝，
  不产生第二份拷贝；
- 按序补发：replay 严格按入队顺序逐条投递，任一投递失败即停止，
  剩余记录保留在队列中等待下次恢复，不丢、不乱序。
"""

from __future__ import annotations

from collections import deque
from typing import Callable, Optional

from .models import ForwardResult, TelemetryRecord


class ReplayBuffer:
    """容量有界的暂存队列；下游恢复后按序补发，重复入队被识别。"""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity 必须为正整数")
        self._capacity = capacity
        self._queue: deque[TelemetryRecord] = deque()
        self._keys: set[tuple[str, int]] = set()

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._queue)

    def pending(self) -> list[TelemetryRecord]:
        """当前暂存内容（按补发顺序），用于观测与测试。"""
        return list(self._queue)

    def enqueue(self, record: TelemetryRecord) -> Optional[str]:
        """入队；返回 None 表示成功，否则返回 'duplicate' 或 'buffer_full'。"""
        key = (record.device_id, record.seq)
        if key in self._keys:
            return "duplicate"
        if len(self._queue) >= self._capacity:
            return "buffer_full"
        self._queue.append(record)
        self._keys.add(key)
        return None

    def replay(self, deliver: Callable[[TelemetryRecord], bool],
               now_ms: int) -> list[ForwardResult]:
        """按入队顺序补发；失败即停，保留剩余待下次恢复。"""
        results: list[ForwardResult] = []
        while self._queue:
            record = self._queue[0]
            if deliver(record):
                self._queue.popleft()
                self._keys.discard((record.device_id, record.seq))
                results.append(ForwardResult(
                    record=record, delivered=True,
                    attempt=len(results) + 1, note="replayed"))
            else:
                results.append(ForwardResult(
                    record=record, delivered=False,
                    attempt=len(results) + 1,
                    note="下游仍不可用，停止补发，剩余保留"))
                break
        return results
