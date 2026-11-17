defmodule TroupeGateway.GatewayTest do
  @moduledoc """
  传输桥语义（移植 troupe-gateway Crystal spec/web_socket_gateway_spec.cr）：
  路由回调、会话元数据、投递/广播、溢出策略、断线清理、static_dir、去重。
  """

  use ExUnit.Case, async: false

  alias TroupeGateway.{Config, Session, WsTestClient}

  defp safe_stop(pid) do
    if Process.alive?(pid) do
      try do
        TroupeGateway.stop(pid)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end
    end
  end

  defp wait_until(timeout \\ 3_000, fun) do
    deadline = System.monotonic_time(:millisecond) + timeout

    loop = fn loop_fn ->
      if fun.() do
        :ok
      else
        if System.monotonic_time(:millisecond) >= deadline do
          raise "wait_until 超时"
        end

        Process.sleep(10)
        loop_fn.(loop_fn)
      end
    end

    loop.(loop)
  end

  defp start_gw(opts \\ []) do
    {:ok, gw} =
      TroupeGateway.start_link(
        Keyword.merge([outbox: TroupeGateway.OutboxSource.ETS.new()], opts)
      )

    {:ok, port} = TroupeGateway.listen(gw, "127.0.0.1", 0)
    on_exit(fn -> safe_stop(gw) end)
    {gw, port}
  end

  # 极简 HTTP GET（本机 OTP 未带 :inets，用原始 TCP 验证兜底路径）
  defp http_get(port, path) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 3_000)

    :ok =
      :gen_tcp.send(sock, "GET #{path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")

    {:ok, raw} = recv_all(sock, <<>>)
    :gen_tcp.close(sock)

    [status_line | rest] = String.split(raw, "\r\n")
    [_, code, _] = String.split(status_line, " ", parts: 3)
    [headers, body] = String.split(Enum.join(rest, "\r\n"), "\r\n\r\n", parts: 2)
    {String.to_integer(code), parse_headers(Map.new(), headers), body}
  end

  defp recv_all(sock, acc) do
    case :gen_tcp.recv(sock, 0, 3_000) do
      {:ok, data} -> recv_all(sock, acc <> data)
      {:error, :closed} -> {:ok, acc}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_headers(acc, text) do
    text
    |> String.split("\r\n")
    |> Enum.reduce(acc, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [k, v] -> Map.put(acc, String.downcase(String.trim(k)), String.trim(v))
        _ -> acc
      end
    end)
  end

  test "on_message 收到上行原文与握手信息，应用可回发" do
    test_pid = self()
    {:ok, gw} = TroupeGateway.start_link(outbox: TroupeGateway.OutboxSource.ETS.new())

    TroupeGateway.set_on_message(gw, fn session, raw ->
      info = Session.info(session)
      send(test_pid, {:msg, info.id, info.path, info.headers["x-test"], raw})
      TroupeGateway.deliver(gw, session, %{"type" => "pong"})
    end)

    {:ok, port} = TroupeGateway.listen(gw, "127.0.0.1", 0)
    on_exit(fn -> safe_stop(gw) end)

    {:ok, client} = WsTestClient.connect("ws://127.0.0.1:#{port}/ws?x=1", %{"X-Test" => "probe"})
    WsTestClient.send_raw(client, ~s({"type":"ping"}))

    assert_receive {:msg, id, path, header, raw}, 3_000
    assert id != ""
    assert path == "/ws"
    assert header == "probe"
    assert raw == ~s({"type":"ping"})

    reply = WsTestClient.expect(client, &(&1["type"] == "pong"))
    assert reply["type"] == "pong"
  end

  test "on_connect 拒绝接入：会话不注册、不触发 on_disconnect" do
    test_pid = self()
    {:ok, gw} = TroupeGateway.start_link(outbox: TroupeGateway.OutboxSource.ETS.new())

    TroupeGateway.set_on_connect(gw, fn session -> Session.info(session).meta["try"] == "ok" end)

    TroupeGateway.set_on_disconnect(gw, fn session ->
      send(test_pid, {:disconnected, session.id})
    end)

    {:ok, port} = TroupeGateway.listen(gw, "127.0.0.1", 0)
    on_exit(fn -> safe_stop(gw) end)

    {:ok, rejected} = WsTestClient.connect("ws://127.0.0.1:#{port}/")
    WsTestClient.send_raw(rejected, ~s({"type":"ping"}))
    Process.sleep(250)
    assert TroupeGateway.sessions_size(gw) == 0
    refute_receive {:disconnected, _}
  end

  test "deliver/broadcast 按会话路由；断线触发 on_disconnect 并清理" do
    test_pid = self()
    {:ok, gw} = TroupeGateway.start_link(outbox: TroupeGateway.OutboxSource.ETS.new())

    TroupeGateway.set_on_disconnect(gw, fn session ->
      send(test_pid, {:disconnected, session.id})
    end)

    TroupeGateway.set_on_message(gw, fn session, raw ->
      send(test_pid, {:identified, raw, Session.info(session).id})
      TroupeGateway.broadcast(gw, %{"type" => "b", "to" => raw}, nil)
    end)

    {:ok, port} = TroupeGateway.listen(gw, "127.0.0.1", 0)
    on_exit(fn -> safe_stop(gw) end)

    {:ok, a} = WsTestClient.connect("ws://127.0.0.1:#{port}/")
    {:ok, b} = WsTestClient.connect("ws://127.0.0.1:#{port}/")

    WsTestClient.send_raw(a, ~s("A"))
    assert_receive {:identified, ~s("A"), a_id}, 3_000
    WsTestClient.send_raw(b, ~s("B"))
    assert_receive {:identified, ~s("B"), b_id}, 3_000

    wait_until(fn -> TroupeGateway.sessions_size(gw) == 2 end)

    assert TroupeGateway.broadcast(gw, %{"type" => "b"}, nil) == 2
    WsTestClient.expect(a, &(&1["type"] == "b"))
    WsTestClient.expect(b, &(&1["type"] == "b"))

    assert TroupeGateway.deliver_id(gw, a_id, %{"type" => "direct"})
    WsTestClient.expect(a, &(&1["type"] == "direct"))
    refute TroupeGateway.deliver_id(gw, "nonexistent", %{"type" => "direct"})

    # 广播排除语义
    assert TroupeGateway.broadcast(gw, %{"type" => "b2"}, a_id) == 1
    WsTestClient.expect(b, &(&1["type"] == "b2"))

    # a 断线：on_disconnect 触发并清理
    WsTestClient.close(a)
    assert_receive {:disconnected, ^a_id}, 3_000
    wait_until(fn -> TroupeGateway.sessions_size(gw) == 1 end)
  end

  test "static_dir：非 WS 请求回静态文件，缺失回 404 JSON" do
    dir = Path.join(System.tmp_dir!(), "gw-static-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "index.html"), "<h1>ok</h1>")
    on_exit(fn -> File.rm_rf!(dir) end)

    {_gw, port} = start_gw(config: [static_dir: dir])

    {code, headers, body} = http_get(port, "/")
    assert code == 200
    assert body == "<h1>ok</h1>"
    assert String.contains?(headers["content-type"], "text/html")

    {code2, headers2, body2} = http_get(port, "/missing.js")
    assert code2 == 404
    assert String.contains?(headers2["content-type"], "application/json")
    assert String.contains?(body2, "troupe-gateway")

    {code3, _headers3, _body3} = http_get(port, "/../secret.txt")
    assert code3 == 404
  end

  test "溢出策略 drop：mailbox 满时丢弃消息、会话保留" do
    config = Config.new(mailbox_cap: 2, push_wait: 50)

    {:ok, gw} =
      TroupeGateway.start_link(outbox: TroupeGateway.OutboxSource.ETS.new(), config: config)

    {:ok, session} = Session.start_link(id: "s1", gateway: gw, config: config)
    TroupeGateway.register(gw, session)

    assert TroupeGateway.deliver(gw, session, ~s({"n":1}))
    assert TroupeGateway.deliver(gw, session, ~s({"n":2}))
    refute TroupeGateway.deliver(gw, session, ~s({"n":3}))
    assert TroupeGateway.sessions_size(gw) == 1
    TroupeGateway.stop(gw)
  end

  test "溢出策略 disconnect：mailbox 满时断开会话并触发 on_disconnect" do
    test_pid = self()
    config = Config.new(mailbox_cap: 2, push_wait: 50, overflow: :disconnect)

    {:ok, gw} =
      TroupeGateway.start_link(
        outbox: TroupeGateway.OutboxSource.ETS.new(),
        config: config,
        on_disconnect: fn session -> send(test_pid, {:disconnected, session.id}) end
      )

    {:ok, session} = Session.start_link(id: "s2", gateway: gw, config: config)
    TroupeGateway.register(gw, session)

    assert TroupeGateway.deliver(gw, session, ~s({"n":1}))
    assert TroupeGateway.deliver(gw, session, ~s({"n":2}))
    refute TroupeGateway.deliver(gw, session, ~s({"n":3}))

    assert_receive {:disconnected, "s2"}, 3_000
    assert TroupeGateway.sessions_size(gw) == 0
    assert Session.closed?(session)
    TroupeGateway.stop(gw)
  end

  test "duplicate? 事件去重；dedup=false 时透传" do
    {:ok, gw} = TroupeGateway.start_link(outbox: TroupeGateway.OutboxSource.ETS.new())
    refute TroupeGateway.duplicate?(gw, "e1")
    assert TroupeGateway.duplicate?(gw, "e1")
    refute TroupeGateway.duplicate?(gw, "e2")
    TroupeGateway.stop(gw)

    {:ok, gw2} =
      TroupeGateway.start_link(
        outbox: TroupeGateway.OutboxSource.ETS.new(),
        config: [dedup: false]
      )

    refute TroupeGateway.duplicate?(gw2, "e1")
    refute TroupeGateway.duplicate?(gw2, "e1")
    TroupeGateway.stop(gw2)
  end
end
