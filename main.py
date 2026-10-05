"""边缘数据接入网关：本地可运行的 HTTP 入口。

端点：
- POST /ingest/{source}   接入原始报文（source 如 http-json / modbus）
- POST /connect/{device_id}    建立设备连接（受并发配额约束）
- POST /disconnect/{device_id} 关闭设备连接
- POST /downstream/{up|down}   本地注入下游故障/恢复
- POST /flush                  触发按序补发
- GET  /config/{device_id}     查询该设备当前生效配置（含版本）
- POST /config                 批量提交配置更新（整批校验，非法整批拒绝）
- GET  /decisions              查询判定台账（按设备/时间段/类型过滤）
- GET  /health                 健康检查
"""
from __future__ import annotations

import time
from typing import Any

import uvicorn
from fastapi import FastAPI, Query
from pydantic import BaseModel

from gateway.audit import DecisionLedger, RecordKind
from gateway.buffer import SpoolBuffer
from gateway.config import ConfigRegistry, DeviceConfig
from gateway.downstream import SimulatedDownstream
from gateway.gateway import Gateway
from gateway.normalizer import HttpJsonAdapter, ModbusAdapter, Normalizer
from gateway.session import SessionManager

BUFFER_CAPACITY = 1000
LEDGER_CAPACITY = 10_000

normalizer = Normalizer()
normalizer.register(HttpJsonAdapter())
normalizer.register(ModbusAdapter())

registry = ConfigRegistry(DeviceConfig(
    device_id=None,
    version=1,
    max_concurrent_sessions=1,
    max_reports_per_window=100,
    window_ms=60_000,
))
sessions = SessionManager(registry=registry)
downstream = SimulatedDownstream()
gateway = Gateway(normalizer, sessions, SpoolBuffer(BUFFER_CAPACITY), downstream,
                  ledger=DecisionLedger(LEDGER_CAPACITY))

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
    reason = gateway.connect(device_id, received_at_ms=now_ms())
    return {"ok": reason is None, "reason": reason.value if reason else None}


@app.post("/disconnect/{device_id}")
def disconnect(device_id: str) -> dict[str, Any]:
    gateway.disconnect(device_id, received_at_ms=now_ms())
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


class ConfigUpdate(BaseModel):
    device_id: str | None = None     # null = 默认配置
    version: int
    max_concurrent_sessions: int
    max_reports_per_window: int
    window_ms: int


class ConfigBatch(BaseModel):
    updates: list[ConfigUpdate]


@app.post("/config")
def apply_config(batch: ConfigBatch) -> dict[str, Any]:
    """批量提交配置更新：整批校验（字段合法 + 版本单调），非法整批拒绝。"""
    updates = [DeviceConfig(device_id=u.device_id, version=u.version,
                            max_concurrent_sessions=u.max_concurrent_sessions,
                            max_reports_per_window=u.max_reports_per_window,
                            window_ms=u.window_ms, applied_at_ms=now_ms())
               for u in batch.updates]
    result = registry.apply_updates(updates)
    return {"applied": result.applied, "errors": list(result.errors),
            "effective": [{"device_id": c.device_id, "version": c.version}
                          for c in result.effective]}


@app.get("/config/{device_id}")
def get_config(device_id: str) -> dict[str, Any]:
    cfg = registry.effective_for(device_id)
    return {"device_id": device_id, "version": cfg.version,
            "override": cfg.device_id is not None,
            "max_concurrent_sessions": cfg.max_concurrent_sessions,
            "max_reports_per_window": cfg.max_reports_per_window,
            "window_ms": cfg.window_ms}


@app.get("/decisions")
def decisions(device_id: str | None = Query(default=None),
              since_ms: int | None = Query(default=None),
              until_ms: int | None = Query(default=None),
              kind: str | None = Query(default=None)) -> dict[str, Any]:
    """查询判定台账：按设备/时间段/记录类型过滤，按事件发生顺序返回。"""
    kinds = [RecordKind(kind)] if kind else None
    assert gateway.ledger is not None
    recs = gateway.ledger.query(device_id=device_id, since_ms=since_ms,
                                until_ms=until_ms, kinds=kinds)
    return {"count": len(recs), "records": [
        {"seq_no": r.seq_no, "kind": r.kind.value, "device_id": r.device_id,
         "decision": r.decision.value, "reason": r.reason,
         "config_version": r.config_version, "received_at_ms": r.received_at_ms,
         "detail": r.detail}
        for r in recs
    ]}


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "buffered": len(gateway.buffer),
            "downstream_up": downstream.is_up,
            "ledger": len(gateway.ledger) if gateway.ledger else 0}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8800)
