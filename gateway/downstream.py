"""本地模拟下游：可注入故障，完整复现下游行为，不依赖任何外部服务。"""

from __future__ import annotations

from .models import TelemetryRecord


class SimulatedDownstream:
    """确定性下游：可切换可用性，记录全部成功投递的流水。"""

    def __init__(self) -> None:
        self._available = True
        self._received: list[TelemetryRecord] = []

    def set_available(self, available: bool) -> None:
        self._available = available

    @property
    def available(self) -> bool:
        return self._available

    def deliver(self, record: TelemetryRecord) -> bool:
        """投递一条记录；不可用时返回 False，不产生副作用。"""
        if not self._available:
            return False
        self._received.append(record)
        return True

    @property
    def received(self) -> list[TelemetryRecord]:
        return list(self._received)
