# troupe-gateway-elixir

Troupe 的**外部端点传输桥**（[troupe-gateway](https://github.com/ShiningRay/troupe-gateway) 的 Elixir 实现）：把"外部客户端 ↔ actor"的通用传输管理收编成开箱即用的伴生包，协议路由留给应用。分层边界与决策记录见 [troupe ADR-0007](https://github.com/ShiningRay/troupe/blob/main/docs/adr/0007-external-endpoint-layering.md)。

## 职责边界（ADR-0007 L3）

**负责**：WS 会话生命周期、出站有界 mailbox + 限时投递 + 溢出策略（drop/disconnect）、Outbox 派发循环、`event_id` 去重、握手信息暴露。

**不负责**：应用协议路由、业务语义（房间/昵称）、认证授权、路由/中间件/序列化（那是 Phoenix/Plug 的领域）。

## 快速开始

```elixir
source = TroupeGateway.OutboxSource.ETS.new()

{:ok, gw} = TroupeGateway.start_link(outbox: source)

# 应用协议路由：上行消息 → actor 调用（setter 风格，对齐 Crystal property）
TroupeGateway.set_on_message(gw, fn session, raw ->
  # 解析 raw → typed actor 调用 → TroupeGateway.deliver(gw, session, 回复)
end)

# Outbox 事件路由（已按 event_id 去重；至少一次由 pending 保留语义保证）
TroupeGateway.set_on_outbox(gw, fn record ->
  # record.topic / record.payload → TroupeGateway.deliver_id / broadcast
end)

TroupeGateway.track(gw, "chat/room/lobby")
{:ok, port} = TroupeGateway.listen(gw, "127.0.0.1", 7777)
```

## API

| 成员 | 说明 |
|---|---|
| `start_link(outbox:, config:, callbacks…)` / `stop` | 启停（回调也可用 setter 逐个挂） |
| `set_on_message / set_on_connect / set_on_disconnect / set_on_outbox` | 应用回调（on_connect 返回 false 拒绝，不触发 on_disconnect） |
| `track / untrack(gw, actor_key)` | 派发循环的轮询对象（逐 actor，与 Crystal 版一致） |
| `deliver(gw, session, payload) :: boolean` | 限时投递；溢出按策略（binary 直发，map 转 JSON） |
| `deliver_id(gw, id, payload) :: boolean` | 按会话 id 投递 |
| `broadcast(gw, payload, except) :: non_neg_integer` | 全体广播（可排除），返回成功数 |
| `duplicate?(gw, event_id) :: boolean` | 事件去重查询 |
| `listen(gw, host, port) :: {:ok, port}` | 启动监听（0 → 系统分配） |
| `sessions_size / tracked_size / session(gw, id)` | 注册表查询 |
| `Session.info(pid) :: %Session{}` | id / meta / path / query / headers |
| `OutboxSource` behaviour | `poll(source, actor_key, limit)` / `mark_delivered(source, actor_key, event_id)` |

`Config`：`mailbox_cap`（默认 256）、`push_wait`（200ms）、`overflow`（`:drop` / `:disconnect`）、`dedup`（true）、`dedup_cache`（4096，超出整表清空）、`dispatch_interval`（50ms）、`poll_batch`（64）、`service_name`、`static_dir`。

## 与 Crystal 版的差异

| 项 | Crystal（troupe-gateway） | 本实现 |
|---|---|---|
| 并发模型 | Fiber + Channel + Mutex | GenServer（Session 单进程持 socket 写权，免锁） |
| WS 实现 | Crystal stdlib `HTTP::WebSocket` | 零依赖 RFC 6455 over `:gen_tcp`（握手/帧/掩码/ping-pong/close） |
| Outbox API | `PropStore#poll_outbox(actor_id)`（Props 同事务提交） | **内置 `OutboxSource` behaviour + ETS 默认实现**（见下） |
| 失败处理 | 回调抛错 → 不 mark_sent → 下轮重 poll | 回调 raise → 不 mark_delivered → pending 保留 → 下轮重投 |
| 限时限队 | `select` + `timeout` | GenServer 延迟应答（满时挂起 caller，`push_wait` 后应答 false） |

**内置 Outbox 说明**（社区裁决）：troupe-elixir 核心当前没有 Outbox/WS 层，按裁决由本包内置
`OutboxSource` behaviour 与 `ETS` 内存实现——`append(source, topic, payload, actor_key:, event_id:)`
即产出事件。与核心 Props 的**原子提交语义不在本包范围**（生产请实现 behaviour 接
claim/lease 型 PG 源，参照 trouperb/troupego 的 outbox 表设计）；跨节点安全亦然。

## 验证

```sh
mix compile --warnings-as-errors
mix test                          # 路由/握手/广播/溢出/去重/断线/静态目录/Outbox 派发
mix run examples/chat_echo.exs    # ws://127.0.0.1:7793/
node scripts/smoke.mjs 7793       # 例程跑起后的端到端冒烟
```

## 关联

- 参照实现：[troupe-gateway（Crystal）](https://github.com/ShiningRay/troupe-gateway)
- 运行时：[troupe-elixir](https://github.com/ShiningRay/troupe-elixir)
- 决策记录：[ADR-0007 外部端点分层](https://github.com/ShiningRay/troupe/blob/main/docs/adr/0007-external-endpoint-layering.md)

## License

MIT，见 [LICENSE](LICENSE)。
