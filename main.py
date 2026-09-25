"""边缘数据接入网关：本地可运行的 HTTP 入口。

端点：
- POST /ingest/{source}   接入原始报文（source 如 http-json / modbus）
- POST /connect/{device_id}    建立设备连接（受并发配额约束）
- POST /disconnect/{device_id} 关闭设备连接
- POST /downstream/{up|down}   本地注入下游故障/恢复
- POST /flush                  触发按序补发
- GET  /health                 健康检查
"""
from __future__ import annotations

import time
from typing import Any

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from gateway.buffer import SpoolBuffer
from gateway.downstream import SimulatedDownstream
from gateway.gateway import Gateway
from gateway.normalizer import HttpJsonAdapter, ModbusAdapter, Normalizer
from gateway.session import QuotaPolicy, SessionManager

BUFFER_CAPACITY = 1000

normalizer = Normalizer()
normalizer.register(HttpJsonAdapter())
normalizer.register(ModbusAdapter())

sessions = SessionManager(QuotaPolicy(
    version=1,
    max_concurrent_sessions_per_device=1,
    max_reports_per_window=100,
    window_ms=60_000,
))
downstream = SimulatedDownstream()
gateway = Gateway(normalizer, sessions, SpoolBuffer(BUFFER_CAPACITY), downstream)

app = FastAPI(title="edge-telemetry-gateway")


def now_ms() -> int:
    return int(time.time() * 1000)


class IngestResponse(BaseModel):
    accepted: bool
    duplicate: bool = False
    reason: str | None = None
    detail: str | None = None


@app.post("/ingest/{source}", response_model=IngestResponse)
def ingest(source: str, payload: dict[str, Any]) -> IngestResponse:
    from gateway.models import RawEnvelope
    result = gateway.ingest(RawEnvelope(source=source, payload=payload, received_at_ms=now_ms()))
    if result.accepted:
        return IngestResponse(accepted=True, duplicate=result.duplicate)
    assert result.rejection is not None
    return IngestResponse(accepted=False,
                          reason=result.rejection.reason.value,
                          detail=result.rejection.detail)


@app.post("/connect/{device_id}")
def connect(device_id: str) -> dict[str, Any]:
    reason = sessions.open_connection(device_id)
    return {"ok": reason is None, "reason": reason.value if reason else None}


@app.post("/disconnect/{device_id}")
def disconnect(device_id: str) -> dict[str, Any]:
    sessions.close_connection(device_id)
    return {"ok": True}


@app.post("/downstream/{state}")
def set_downstream(state: str) -> dict[str, Any]:
    if state not in ("up", "down"):
        return {"ok": False, "reason": "state must be up|down"}
    downstream.set_up(state == "up")
    if state == "up":
        gateway.flush()
    return {"ok": True, "is_up": downstream.is_up}


@app.post("/flush")
def flush() -> dict[str, Any]:
    return {"flushed": gateway.flush(), "buffered": len(gateway.buffer)}


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "buffered": len(gateway.buffer), "downstream_up": downstream.is_up}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8800)
