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
| `gateway/gateway.py` | 编排：归一 → 去重/缓存 → 会话配额 → 转发/补发，判定留痕 |
| `gateway/config.py` | 按设备配置治理：差异化配额、版本化热更新、原子批量应用、有界历史快照 |
| `gateway/audit.py` | 判定追溯：有界判定日志、按设备/时间查询、确定性重放 |
| `main.py` | FastAPI HTTP 入口（本地运行） |
| `tests/` | 34 项单元/集成测试 |

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

## 配置治理与判定追溯

### 按设备配置与热更新

配置由 `ConfigRegistry` 统一管理：一份**默认配置** + 若干**设备覆盖**
（`DeviceConfig = {max_concurrent_sessions_per_device, max_reports_per_window,
window_ms}`）。未覆盖的设备使用默认配置；覆盖只影响目标设备，其他设备照常接入。

```bash
# 查看当前生效配置
curl localhost:8800/config

# 热更新：version 必须单调前进；devices 里值为 null 表示移除该设备覆盖
curl -X POST localhost:8800/config -H 'content-type: application/json' -d '{
  "version": 2,
  "devices": {"d1": {"max_concurrent_sessions_per_device": 1,
                     "max_reports_per_window": 10, "window_ms": 60000}}
}'
```

规则：

- **立即生效、无需重启**：`apply` 成功后下一条上报即按新版本判定；
- **原子批量**：一批变更（默认配置 + 多设备覆盖）先整体校验再一次性替换。
  任一配置非法（非正整数等）或 `version` 未前进 → 整批拒绝（HTTP 409），
  原生效配置（包括同批其他合法设备项）完全不变；
- **与上报/补发并发安全**：单次判定的「取生效配置 → 会话/配额判定 → 留痕」
  在同一锁内完成，配置切换不会插入单次判定中间，同一批数据不会被新旧两套
  配置重复判定，也不会丢失或打乱补发顺序；
- **历史有界**：注册表保留最近 32 个版本快照（`history_limit`），超出淘汰最旧。

### 判定如何被记录、查询与复现

每条上报的判定（`accepted` / `duplicate` / `rejected`+原因）连同**当时生效的
配置版本与配置快照**写入 `AuditLog`；建连/断连事件同样记录（重放时还原会话
状态）。每条记录带全局单调 `seq_no`。

```bash
# 按设备 / 时间段查询判定记录
curl 'localhost:8800/audit?device_id=d1&start_ms=1800000000000&end_ms=1800000001000'
```

- **重放复现**：`gateway.audit.replay(entries)` 用每条记录里钉住的配置快照，
  在全新构造的归一器/缓存/会话状态上按 `seq_no` 顺序原样重跑判定流水线，
  逐项比对 `accepted/duplicate/rejected+原因`——全部一致（返回空列表）才算
  复现成功。重放结论与当前生效配置无关。
- **已受理未转发的数据**：判定发生在受理时刻，补发（flush）只按 offset 顺序
  转发，不按新配置重判；因此改配置后历史数据的结论与补发顺序都不变。
- **日志有界**：`AuditLog` 容量固定（默认 10000），超出淘汰最旧记录并计入
  `dropped`（`/audit` 与 `/health` 可见），内存不无限增长。注意：被淘汰后
  剩余的历史残片不保证可完整重放（重放需要从建连事件起的全量记录）。

### 兼容范围

- **旧代码路径不受影响**：不显式传 `ConfigRegistry`/`AuditLog` 时，`Gateway`
  用会话层全局策略自建注册表，`sessions.update_policy` 的旧接口会自动同步为
  新的默认配置版本，既有 25 项测试语义不变；
- **旧配置下产生的数据**：判定记录里钉的是受理时刻的配置快照，重放与审计
  永远按当时的配置语义解释，不受后续配置变更影响；
- **旧的缓存数据**：补发不重新判定，缓存中的记录在任意配置版本下都按
  受理时的结论转发；
- **重放的归一规则**：重放使用与网关同源的归一器（`http-json`/`modbus`
  适配器），归一规则随代码版本演进时需与历史记录同代解释。

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
`GET/POST /config`（查看/热更新配置）、`GET /audit`（判定追溯查询）、
`GET /health`。

## 验证方法

```bash
uv run pytest -q                    # 34 项全部通过
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
- **HTTP 全链路**：TestClient 覆盖建连、上报、故障注入、自动补发、各类拒绝；
- **按设备差异化配置**：不同设备不同并发/频率上限，互不影响；
- **热更新**：改配置立即对目标设备生效，前后判定对照及各自钉住的配置版本可查；
- **判定重放**：混合场景（接受/重复/配额/序列回退/断连/跨配置版本）全量重放
  零偏差；
- **受理语义钉版**：已受理未转发数据在配置变更后补发不重判、顺序不变；
- **并发确定性**：配置切换与并发上报/补发同时进行，无异常、无丢失、offset
  唯一有序、全量记录重放复现；
- **非法配置**：版本倒退或任一非法项整批拒绝，原配置（含同批合法项）不变；
- **追溯日志有界**：容量上限、淘汰计数、按设备/时间段查询。

日志样例（输入 → 判定 → 补发流水）：

```
input envelope: source=http-json payload={...seq: 2} received_at_ms=...
stored device=d1 metric=temp seq=2
forwarded offset=0 device=d1 seq=0
forwarded offset=1 device=d1 seq=1
forwarded offset=2 device=d1 seq=2
```
