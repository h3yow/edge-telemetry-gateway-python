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
| `gateway/config.py` | 按设备配置治理：差异化配置、整批校验热更新、有界版本历史 |
| `gateway/session.py` | 设备会话、并发连接配额、滑窗频率配额、序列连续性检查（按设备生效配置判定） |
| `gateway/buffer.py` | 有界本地缓存：容量上限、内容去重、单调 offset、确认截断；记录受理时配置版本 |
| `gateway/audit.py` | 判定台账（有界、可查询）与确定性重放 |
| `gateway/downstream.py` | 下游协议与本地故障注入实现 |
| `gateway/gateway.py` | 编排：归一 → 去重/缓存 → 会话配额 → 转发/补发；判定/会话/补发事件入台账 |
| `main.py` | FastAPI HTTP 入口（本地运行） |
| `tests/` | 33 项单元/集成测试 |

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

## 配置治理与判定可追溯

### 按设备配置与热更新

- 配置由 `ConfigRegistry` 管理：一份**默认配置**（`device_id=null`）+ 任意多份
  **设备覆盖**。未覆盖的设备回落默认配置；覆盖只影响目标设备，其他设备的
  会话、窗口、判定完全不受影响。
- 每台设备的配置是一条独立版本链，`version` 单调递增（设备首次覆盖自成一链，
  与默认链版本无关）。
- 更新按批提交（`POST /config`）：**先整批校验、后整批生效**。任一更新字段非法
  （配额/窗口 < 1）或版本不前进（持平/回退），整批拒绝并返回全部错误，
  已生效配置保持不变——不会出现部分生效。
- 热更新无需重启：判定路径（上报/建连/补发）与配置更新共用同一把锁，
  单条报文的判定要么完全用旧版本、要么完全用新版本；配置切换与并发上报、
  补发同时进行时，不会出现同一批数据被新旧两套配置重复判定，补发顺序
  （offset）不受配置切换影响。
- 配置版本切换后，该设备（且仅该设备）的频率窗口在新版本下重新起算，
  序列连续性（`last_seq`）保留。

```bash
# d1 覆盖：窗口配额放宽到 10（版本 2 必须大于 d1 链当前版本）
curl -X POST localhost:8800/config -H 'content-type: application/json' -d '{
  "updates": [{"device_id": "d1", "version": 2, "max_concurrent_sessions": 1,
               "max_reports_per_window": 10, "window_ms": 60000}]}'
curl localhost:8800/config/d1     # 查 d1 生效配置（含版本、是否覆盖）
```

### 判定台账与查询

每个事件——上报判定（接受/重复/拒绝）、建连/断连、转发补发——都作为一条
`DecisionRecord` 写入有界台账 `DecisionLedger`：

```
(seq_no, kind, device_id, decision, reason, config_version, received_at_ms, envelope, detail)
```

- `config_version` 是**事件发生时该设备生效的配置版本**，即判定依据；
- `envelope` 保留原始报文，出处完整；
- 台账容量固定（默认 10000），超出淘汰最旧记录，内存不会无限增长；
- 补发流水（`kind=forward`）记录的是**受理时**的版本：已收下未转发的数据
  在补发时不重新判定，配置后续变更不改变已受理数据的语义。

```bash
curl 'localhost:8800/decisions?device_id=d1&since_ms=1800000000000&kind=decision'
```

### 历史判定重放

`audit.replay(records, registry, normalizer)` 把台账记录原样重放：

1. 按 `seq_no` 升序取记录（补发流水不参与判定重放）；
2. 每条记录生效的配置版本从 `ConfigRegistry` 的**版本历史**中取回
   （每设备保留有界历史，默认 16 版），构造冻结视角的 `SessionManager`；
3. 用相同的归一化器、容量不受限的缓存重放判定，逐条与当时结论对照。

重放复现的是**配置敏感判定**（归一化、去重、序列、会话、配额）。
`buffer_full` 由下游瞬时可用性驱动、不属于配置语义，重放时缓存容量不受限。
配置版本历史或台账记录被淘汰后，对应区间无法忠实重放（`ReplayError`）——
容量即追溯窗口的边界。

### 兼容范围

- **旧 API**：`SessionManager(QuotaPolicy(...))` 与 `update_policy` 仍然可用，
  内部转为默认配置链上的版本化更新；既有 25 项测试全部保持通过。
- **旧缓存数据**：引入版本标记前入缓存的记录 `config_version=0`，仅表示
  "无版本信息"，不影响去重、补发顺序等既有语义；补发不重新判定，
  旧数据按原样转发。
- **旧配置下产生的判定**：只要对应版本还在版本历史内，就可按版本取回
  当时配置重放复现；设备覆盖生效前的历史判定记的是默认链版本，
  重放时自动回落默认链。

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
`POST /config`（批量热更新配置）、`GET /config/{device_id}`（查生效配置）、
`GET /decisions`（查判定台账）、`GET /health`。

## 验证方法

```bash
uv run pytest -q                    # 33 项全部通过
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
- **按设备配置**：不同设备不同并发/频率上限，互不影响
  （`test_per_device_differential_config`）；
- **热更新**：更新前后同一设备判定对照，只影响目标设备
  （`test_hot_update_changes_only_target_device`）；
- **判定重放**：跨版本历史记录重放，结论与当时一致
  （`test_replay_reproduces_historical_decisions`）；
- **并发确定性**：配置切换与并发上报/补发并行，版本单调、不丢不重、补发有序
  （`test_config_switch_concurrent_with_ingest_and_flush_is_deterministic`）；
- **整批拒绝**：非法字段/版本不前进整批拒绝，原配置不变
  （`test_invalid_or_regressing_batch_rejected_atomically`）；
- **受理版本语义**：已受理数据补发按受理时版本入账
  （`test_buffered_data_replayed_with_acceptance_version`）；
- **台账有界**：容量固定，超出淘汰最旧（`test_ledger_capacity_is_bounded`）；
- **HTTP 全链路**：TestClient 覆盖建连、上报、故障注入、自动补发、各类拒绝。

日志样例（输入 → 判定 → 补发流水）：

```
input envelope: source=http-json payload={...seq: 2} received_at_ms=...
stored device=d1 metric=temp seq=2
forwarded offset=0 device=d1 seq=0
forwarded offset=1 device=d1 seq=1
forwarded offset=2 device=d1 seq=2
```
