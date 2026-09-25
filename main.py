"""边缘数据接入网关的本地 HTTP 入口。

仅做薄封装：核心逻辑全部在 gateway 包内，可脱离 HTTP 独立测试与复现。
"""

import time

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from gateway import Gateway, QuotaConfig

app = FastAPI(title="edge-telemetry-gateway")
gateway = Gateway(QuotaConfig())


def _now_ms() -> int:
    return int(time.time() * 1000)


class IngestRequest(BaseModel):
    raw: str
    protocol: str
    device_id: str
    session_id: str


@app.post("/sessions/{device_id}/{session_id}")
def open_session(device_id: str, session_id: str):
    r = gateway.open_session(device_id, session_id)
    return {"accepted": r.accepted, "reason": r.reason.value if r.reason else None,
            "detail": r.detail}


@app.post("/ingest")
def ingest(req: IngestRequest):
    r = gateway.ingest(req.raw, req.protocol, req.device_id,
                       req.session_id, _now_ms())
    return {"accepted": r.accepted, "reason": r.reason.value if r.reason else None,
            "detail": r.detail}


@app.post("/downstream/{available}")
def set_downstream(available: bool):
    flow = gateway.set_downstream_available(available, _now_ms())
    return {"replayed": [
        {"device": f.record.device_id, "seq": f.record.seq,
         "delivered": f.delivered, "note": f.note} for f in flow]}


@app.get("/status")
def status():
    return {"buffered": len(gateway.buffer),
            "downstream_available": gateway.downstream.available,
            "delivered_total": len(gateway.downstream.received),
            "quota_version": gateway.quota.version}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
