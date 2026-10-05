"""边缘数据接入网关：本地可运行的 HTTP 入口。

端点：
- POST /ingest/{source}   接入原始报文（source 如 http-json / modbus）
- POST /connect/{device_id}    建立设备连接（按该设备生效配置的并发配额）
- POST /disconnect/{device_id} 关闭设备连接
- POST /downstream/{up|down}   本地注入下游故障/恢复
- POST /flush                  触发按序补发
- GET  /config                 查看当前生效配置（版本/默认/设备覆盖）
- POST /config                 热更新配置（整批校验，非法或版本倒退整批拒绝）
- GET  /audit                  查询判定追溯记录（可按设备/时间段过滤）
- GET  /health                 健康检查
"""
from __future__ import annotations

import time
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from gateway.audit import AuditLog
from gateway.buffer import SpoolBuffer
from gateway.config import ConfigError, ConfigRegistry, ConfigUpdate, DeviceConfig
from gateway.downstream import SimulatedDownstream
from gateway.gateway import Gateway
from gateway.normalizer import HttpJsonAdapter, ModbusAdapter, Normalizer
from gateway.session import QuotaPolicy, SessionManager

BUFFER_CAPACITY = 1000
AUDIT_CAPACITY = 10_000
CONFIG_HISTORY_LIMIT = 32

normalizer = Normalizer()
normalizer.register(HttpJsonAdapter())
normalizer.register(ModbusAdapter())

sessions = SessionManager(QuotaPolicy(
    version=1,
    max_concurrent_sessions_per_device=1,
    max_reports_per_window=100,
    window_ms=60_000,
))
configs = ConfigRegistry(
    DeviceConfig(max_concurrent_sessions_per_device=1,
                 max_reports_per_window=100, window_ms=60_000),
    version=1, history_limit=CONFIG_HISTORY_LIMIT)
audit_log = AuditLog(capacity=AUDIT_CAPACITY)
downstream = SimulatedDownstream()
gateway = Gateway(normalizer, sessions, SpoolBuffer(BUFFER_CAPACITY), downstream,
                  config_registry=configs, audit_log=audit_log)

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
    reason = gateway.connect(device_id, at_ms=now_ms())
    return {"ok": reason is None, "reason": reason.value if reason else None}


@app.post("/disconnect/{device_id}")
def disconnect(device_id: str) -> dict[str, Any]:
    gateway.disconnect(device_id, at_ms=now_ms())
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


# -- 配置治理 -------------------------------------------------------------


class DeviceConfigModel(BaseModel):
    max_concurrent_sessions_per_device: int
    max_reports_per_window: int
    window_ms: int


class ConfigUpdateModel(BaseModel):
    version: int
    default: DeviceConfigModel | None = None
    # 值为 null 表示移除该设备覆盖
    devices: dict[str, DeviceConfigModel | None] = {}


def _config_view() -> dict[str, Any]:
    snap = configs.snapshot_for_version(configs.version)
    assert snap is not None
    return {
        "version": snap.version,
        "default": vars(snap.default),
        "devices": {d: vars(c) for d, c in snap.devices.items()},
    }


@app.get("/config")
def get_config() -> dict[str, Any]:
    return _config_view()


@app.post("/config")
def apply_config(update: ConfigUpdateModel) -> dict[str, Any]:
    try:
        configs.apply(ConfigUpdate(
            version=update.version,
            default=DeviceConfig(**vars(update.default)) if update.default else None,
            devices={d: (DeviceConfig(**vars(c)) if c is not None else None)
                     for d, c in update.devices.items()},
        ))
    except ConfigError as e:
        # 整批拒绝，原生效配置不变
        raise HTTPException(status_code=409, detail=str(e))
    return {"ok": True, "config": _config_view()}


# -- 判定追溯 -------------------------------------------------------------


@app.get("/audit")
def query_audit(device_id: str | None = None, start_ms: int | None = None,
                end_ms: int | None = None) -> dict[str, Any]:
    entries = audit_log.query(device_id=device_id, start_ms=start_ms, end_ms=end_ms)
    return {
        "capacity": audit_log.capacity,
        "dropped": audit_log.dropped,
        "count": len(entries),
        "entries": [
            {
                "seq_no": e.seq_no,
                "kind": e.kind.value,
                "device_id": e.device_id,
                "received_at_ms": e.received_at_ms,
                "config_version": e.config_version,
                "decision": e.decision.value if e.decision else None,
                "reason": e.reason.value if e.reason else None,
            }
            for e in entries
        ],
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "buffered": len(gateway.buffer),
            "downstream_up": downstream.is_up,
            "config_version": configs.version,
            "audit_entries": len(audit_log)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8800)
