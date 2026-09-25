"""下游抽象与本地可复现的故障注入下游。"""
from __future__ import annotations

from typing import Protocol

from .buffer import BufferedRecord


class Downstream(Protocol):
    """下游接收端协议。"""

    def send(self, record: BufferedRecord) -> bool:
        """返回 True 表示已接收确认。"""
        ...


class SimulatedDownstream:
    """本地模拟下游：可注入故障，完整记录接收流水，便于测试复现。"""

    def __init__(self) -> None:
        self._up: bool = True
        self.received: list[BufferedRecord] = []

    def set_up(self, up: bool) -> None:
        self._up = up

    @property
    def is_up(self) -> bool:
        return self._up

    def send(self, record: BufferedRecord) -> bool:
        if not self._up:
            return False
        self.received.append(record)
        return True
