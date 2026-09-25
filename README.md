# 边缘数据接入网关（edge-telemetry-gateway）

本地可运行的边缘数据接入网关：把多种来源的设备报文归一到统一内部模型，
按设备会话与配额受理，下游不可用时可靠暂存并在恢复后按序补发。
全程不依赖外部服务、真实凭据或付费资源，下游行为由内置模拟下游完整复现。

## 运行与验证

```bash
uv sync                 # 安装依赖
uv run pytest -q -s     # 运行全部测试（-s 可看到输入/判定/补发日志）
uv run python main.py   # 启动本地 HTTP 服务（127.0.0.1:8000）
```

HTTP 接口（薄封装，核心逻辑在 `gateway/` 包内，可脱离 HTTP 测试）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/sessions/{device_id}/{session_id}` | 建立设备会话 |
| POST | `/ingest` | 接入报文 `{raw, protocol, device_id, session_id}` |
| POST | `/downstream/{available}` | 切换下游可用性，恢复时返回补发流水 |
| GET  | `/status` | 暂存量、下游状态、已投递总数、配额版本 |

## 接入协议与归一

三种本地报文格式统一归一到 `TelemetryRecord`
（device_id / seq / timestamp_ms / metrics / source_protocol / raw / config_version）：

- **json**: `{"device":"d1","seq":1,"ts":1700000000000,"metrics":{"t":20.5}}`
- **csv**: `d1,1,1700000000000,t=20.5,h=0.4`
- **kv**:  `device=d1;seq=1;ts=1700000000000;t=20.5`

归一失败的报文**不静默丢弃**，返回可区分原因并附原始出处（协议名 + 原始报文）：

| 拒绝原因 | 触发条件 |
|---|---|
| `unknown_protocol` | 协议名不在 json/csv/kv |
| `malformed_payload` | 报文无法按协议解析、seq/ts 非整数、指标值非数值 |
| `missing_field` | 缺少 device / seq / ts |
| `timestamp_anomaly` | 时间戳超前 >5 分钟或落后 >30 天 |
| `sequence_regression` | 同设备 seq 未严格递增（含同号重发、迟到旧报文） |
| `session_conflict` | 并发连接超限、会话不存在、报文设备与连接设备不一致 |
| `quota_exceeded` | 滑动窗口内上报次数超配额 |
| `buffer_full` | 暂存队列满 |
| `duplicate` | 同一 (device_id, seq) 重复进入暂存队列 |

## 缓存与补发规则

- **有界暂存**：`ReplayBuffer(capacity)` 容量固定，满时新记录以 `buffer_full`
  拒绝，内存不会无上限增长；拒绝不消耗该设备的序列号与配额，腾出空间后同号重发可受理。
- **去重**：以 `(device_id, seq)` 为键，重复入队返回 `duplicate`，只保留一份。
- **不插队**：下游可用但暂存非空时，新记录仍入队排队，保证全局按序。
- **按序补发**：下游恢复时严格按入队顺序逐条投递；任一投递失败即停止，
  剩余记录完整保留待下次恢复——不丢、不乱序。补发流水（`ForwardResult`）
  记录每条的 attempt / delivered / note。

## 会话与配额语义

- 每台设备并发会话数受 `max_connections_per_device` 限制，超限或重复建立
  以 `session_conflict` 拒绝；上报必须携带本设备活跃会话。
- 同设备 `seq` 必须严格递增；频率按滑动窗口（`window_ms` /
  `max_reports_per_window`）限流。
- 配额判定只影响超限设备本身，其他设备接入不受影响。
- 配置变更要求 `version` 严格递增；每条记录归一时打上 `config_version`，
  历史数据保留旧版本语义，结果可复现、可解释。
- 会话判定采用**校验-提交两阶段**：先 `check` 校验，投递/暂存成功后才
  `commit` 推进序列号与计数，被拒绝的报文不污染设备状态。

## 测试覆盖（tests/）

- `test_protocols.py`：三种协议归一、字段缺失、时间戳异常、畸形报文、未知协议；
- `test_downstream_recovery.py`：下游故障暂存、恢复按序补发、补发中断保留剩余、
  暂存耗尽拒绝、故障期间无静默丢失（接受+拒绝=提交）；
- `test_duplicates_ordering.py`：序列回退、乱序迟到、故障期重复投递去重、
  被拒报文不消耗序列号；
- `test_quota.py`：频率限流与窗口滑动、并发连接上限、无会话上报、
  设备间隔离、配置版本递增与历史语义复现。

单测日志统一打印 `[输入]`（原始报文与上下文）、`[判定]`（接受/拒绝及依据、出处）、
`[补发]`（恢复时的补发流水），便于人工核对每条结果的判定理由。

## 目录结构

```
gateway/
  models.py      统一数据模型与拒绝原因枚举
  protocols.py   多协议报文归一
  session.py     设备会话与配额（校验-提交两阶段）
  buffer.py      有界暂存与按序补发
  downstream.py  本地模拟下游（可注入故障）
  gateway.py     编排：归一 -> 会话/配额 -> 转发或暂存 -> 补发
main.py          FastAPI 薄封装
tests/           场景化测试（含输入/判定/补发日志）
```
