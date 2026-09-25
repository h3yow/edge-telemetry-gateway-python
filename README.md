# 边缘数据接入网关（本地可运行）

把多种来源的设备数据统一接管、归一、有界缓存并转发到下游；在下游短暂不可用、
报文重复到达或乱序恢复时，各接入链路的最终结果一致、可解释。

- 纯本地运行，**不依赖外部服务、真实凭据或付费资源**；
- 下游为内置的 `SimulatedDownstream`，故障与恢复通过一个 HTTP 调用即可注入；
- 技术栈：Python 3.14 + FastAPI + pytest。

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `gateway/models.py` | 统一内部模型 `TelemetryPoint`、原始报文 `RawEnvelope`、拒绝原因 `RejectReason`、判定结果 `IngestResult` |
| `gateway/normalizer.py` | 协议归一（来源适配器），字段/时间戳/来源校验 |
| `gateway/session.py` | 设备会话、并发连接配额、滑窗频率配额、序列连续性检查、配额版本化 |
| `gateway/buffer.py` | 有界本地缓存：容量上限、内容去重、单调 offset、确认截断 |
| `gateway/downstream.py` | 下游协议与本地故障注入实现 |
| `gateway/gateway.py` | 编排：归一 → 去重/缓存 → 会话配额 → 转发/补发 |
| `main.py` | FastAPI HTTP 入口（本地运行） |
| `tests/` | 25 项单元/集成测试 |

## 接入协议

### HTTP JSON（`http-json`）

`POST /ingest/http-json`

```json
{"device_id": "d1", "metric": "temp", "value": 21.5,
 "ts_ms": 1800000000000, "seq": 0}
```

### Modbus 轮询（`modbus`）

`POST /ingest/modbus`

```json
{"unit": 7, "register": 4, "raw": 23,
 "poll_ts_ms": 1800000000000, "poll_seq": 0}
```

归一映射：`device_id=modbus-{unit}`、`metric=register.{register}`、
`value=float(raw)`、`ts_ms=poll_ts_ms`、`seq=poll_seq`。

### 归一后的统一模型

```
TelemetryPoint(device_id, metric, value, ts_ms, seq, source, received_at_ms)
```

字段缺失/类型非法、时间戳不可解析或与网关接收时刻偏差超过容忍窗口（默认 1 天）、
以及未注册来源都**不会静默丢弃**，而是返回带原因与原始报文的拒绝：

| 拒绝原因 (`reason`) | 判定依据 |
| --- | --- |
| `missing_field` | 必填字段缺失或类型非法（详情指出字段名） |
| `bad_timestamp` | 时间戳不是非负整数毫秒，或超出与接收时刻的偏差容忍窗口 |
| `unknown_source` | 来源没有注册适配器 |
| `sequence_regression` | 同设备 `seq <= last_seq` 且报文内容不是完全重复 |
| `quota_exceeded` | 并发连接数或滑窗上报条数超配额 |
| `session_conflict` | 设备无活跃连接即上报 |
| `buffer_full` | 缓存容量耗尽（不覆盖旧数据） |

每个拒绝都保留 `RawEnvelope`（来源、原始 payload、接收时刻），保证出处可追溯。

## 缓存与补发规则

1. **有界**：缓存容量固定（默认 1000，测试夹具为 4）。满时新报文以
   `buffer_full` 明确拒绝，不覆盖、不静默丢弃，内存无上限增长。
2. **补发顺序确定**：每条记录分配进程内单调递增 `offset`，补发严格按 `offset`
   升序——即按网关接收顺序，与设备 `seq`、时钟波动、到达乱序无关。
3. **故障暂存**：下游 `send` 返回失败时停止本轮补发，数据全部留在缓存；
   恢复后调用 `flush`（或上报时自动触发）继续补发。已确认（ack）的记录才截断，
   因此补发中断后重启不会丢数据，也不会重复补发已确认数据。
4. **重复投递幂等**：去重键 =
   `(device_id, metric, seq, value, ts_ms)`（内容键）。
   - 内容完全相同的报文，无论在缓存中还是已确认（上游重发历史报文），
     都识别为 `duplicate=true`，幂等接受、不重复补发；
   - 同 `seq` 但 `value/ts_ms` 不同，视为序列回退，判 `sequence_regression`；
   - 已确认键保存在**有界**最近确认集（大小 = 容量），避免内存无限增长。
5. **乱序恢复**：下游恢复后缓存按 `offset` 顺序补发，多设备交错数据保持全局顺序。

## 设备会话与配额语义

- 上报前必须先 `POST /connect/{device_id}` 建立连接，否则 `session_conflict`；
  `POST /disconnect/{device_id}` 释放。
- **并发连接配额**：默认每设备 1 个连接（`max_concurrent_sessions_per_device`），
  超出返回 `quota_exceeded`，拒绝只针对该设备，不影响其他设备。
- **频率配额**：滑窗（`window_ms`，默认 60 s）内最多
  `max_reports_per_window`（默认 100）条，按网关接收时刻计窗。
- **接收顺序一致**：同设备 `seq` 必须严格递增；回退/重复序列的报文被拒绝
  （完全相同的重投除外，按幂等重复处理）。
- **配置可复现**：`QuotaPolicy.version` 单调递增。更新配置时，序列连续性
  （`last_seq`，设备固有状态）保留，频率窗口计数在新版本下重新起算；
  历史判定发生时的策略版本记录在会话状态上，历史数据语义可复现。

## 本地运行

```bash
uv sync
uv run python main.py        # http://127.0.0.1:8800
```

手动复现故障→暂存→补发：

```bash
curl -X POST localhost:8800/connect/d1
TS=$(python -c 'import time;print(int(time.time()*1000))')
curl -X POST localhost:8800/downstream/down
curl -X POST localhost:8800/ingest/http-json \
  -H 'content-type: application/json' \
  -d "{\"device_id\":\"d1\",\"metric\":\"temp\",\"value\":1,\"ts_ms\":$TS,\"seq\":0}"
curl localhost:8800/health            # buffered=1
curl -X POST localhost:8800/downstream/up   # 恢复并自动按序补发
curl localhost:8800/health            # buffered=0
```

其他端点：`POST /flush`（触发补发）、`POST /ingest/{source}`、
`GET /health`。

## 验证方法

```bash
uv run pytest -q                    # 25 项全部通过
uv run pytest --log-cli-level=INFO  # 打印每条用例的输入、判定依据与补发流水
```

测试覆盖：

- **下游故障恢复**：故障期间数据全部暂存，恢复后按 `offset` 顺序补发且仅一次
  （`test_downstream_outage_buffered_then_replayed_in_order`）；
- **重复投递**：故障期间重投、补发确认后重投，均幂等且补发流水无重复；
- **乱序到达**：按接收顺序（offset）补发，多设备交错保持全局顺序；
- **序列回退**：回退/同序列不同内容被明确拒绝为 `sequence_regression`；
- **配额冲突**：频率/并发超限拒绝原因可区分，且不影响其他设备；
- **缓存耗尽**：`buffer_full` 明确拒绝，补发腾出空间后可重试成功；
- **协议归一**：缺字段、坏时间戳、未知来源、多协议字段映射；
- **HTTP 全链路**：TestClient 覆盖建连、上报、故障注入、自动补发、各类拒绝。

日志样例（输入 → 判定 → 补发流水）：

```
input envelope: source=http-json payload={...seq: 2} received_at_ms=...
stored device=d1 metric=temp seq=2
forwarded offset=0 device=d1 seq=0
forwarded offset=1 device=d1 seq=1
forwarded offset=2 device=d1 seq=2
```
