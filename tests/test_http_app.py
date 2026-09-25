"""通过 FastAPI TestClient 验证本地 HTTP 入口的完整链路：
建连 -> 正常上报 -> 故障注入 -> 暂存 -> 恢复补发 -> 重复/配额拒绝。
"""
from __future__ import annotations

from fastapi.testclient import TestClient


def test_http_end_to_end():
    import importlib
    import main
    importlib.reload(main)  # 保证全局网关状态独立
    client = TestClient(main.app)

    assert client.post("/connect/d1").json()["ok"] is True
    payload = {"device_id": "d1", "metric": "temp", "value": 21.5,
               "ts_ms": 1_800_000_000_000, "seq": 0}
    # 时间戳超出网关本地时钟容忍窗口会被拒（真实 main 用 now()），改为当前时刻
    import time
    payload["ts_ms"] = int(time.time() * 1000)
    ok = client.post("/ingest/http-json", json=payload)
    assert ok.json()["accepted"] is True

    # 下游故障 -> 暂存
    client.post("/downstream/down")
    payload["seq"] = 1
    payload["ts_ms"] = int(time.time() * 1000)
    assert client.post("/ingest/http-json", json=payload).json()["accepted"] is True
    # 完全重复投递 -> duplicate
    dup = client.post("/ingest/http-json", json=payload).json()
    assert dup["accepted"] is True and dup["duplicate"] is True

    # 序列回退 -> 明确拒绝原因
    payload["seq"] = 0
    back = client.post("/ingest/http-json", json=payload).json()
    assert back["accepted"] is False and back["reason"] == "sequence_regression"

    # 恢复 -> 自动补发
    up = client.post("/downstream/up").json()
    assert up["is_up"] is True
    assert client.get("/health").json()["buffered"] == 0

    # 未知来源
    assert client.post("/ingest/mqtt", json=payload).json()["reason"] == "unknown_source"

    # 并发连接配额
    assert client.post("/connect/d1").json()["reason"] == "quota_exceeded"
