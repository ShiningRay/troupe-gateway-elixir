# 最小示例：echo 网关（对应 Crystal 版 examples/chat_echo.cr）。
# 内置 ETS Outbox 源：上行消息 → 回 echo + 产出事件；派发循环 → 广播。
# 运行：mix run examples/chat_echo.exs
alias TroupeGateway.OutboxSource

source = OutboxSource.ETS.new()

{:ok, gw} = TroupeGateway.start_link(outbox: source)

TroupeGateway.set_on_message(gw, fn session, raw ->
  TroupeGateway.deliver(gw, session, %{"type" => "echo", "text" => raw})
  OutboxSource.ETS.append(source, "echo", %{"text" => raw}, actor_key: "demo/echo/1")
end)

TroupeGateway.set_on_outbox(gw, fn record ->
  TroupeGateway.broadcast(
    gw,
    %{"type" => "event", "topic" => record.topic, "payload" => record.payload},
    nil
  )
end)

TroupeGateway.track(gw, "demo/echo/1")
{:ok, port} = TroupeGateway.listen(gw, "127.0.0.1", 7793)
IO.puts("troupe-gateway chat_echo listening: ws://127.0.0.1:#{port}/")

Process.sleep(:infinity)
