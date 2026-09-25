"""本地有界缓存与按序补发（骨架）。"""
from __future__ import annotations

import enum
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


class SpoolBuffer:
    """有界暂存：容量上限、去重、按 offset 顺序补发、确认后截断。"""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity

    def __len__(self) -> int:
        raise NotImplementedError

    def store(self, point: TelemetryPoint) -> StoreResult:
        raise NotImplementedError

    def pending(self, limit: int | None = None) -> list[BufferedRecord]:
        """按 offset 升序返回待补发记录。"""
        raise NotImplementedError

    def ack_through(self, offset: int) -> int:
        """确认 <= offset 的记录并移除，返回移除条数。"""
        raise NotImplementedError
