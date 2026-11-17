defmodule TroupeGateway do
  @moduledoc """
  WebSocket 网关：外部客户端 ↔ actor 的传输桥（ADR-0007 L3）。
  语义对齐 troupe-gateway（Crystal shard）v0.1.3。

  职责（仅传输管理，协议路由归应用）：

  * 会话生命周期：注册/断线清理/:on_connect 接纳；
  * 出站投递：每会话有界 mailbox + 限时入队 + 溢出策略（:drop/:disconnect）；
  * Outbox 派发循环：track 的 actor 每 `:dispatch_interval` 轮询一次，
    事件（至少一次）经可选 event_id 去重后交 `:on_outbox` 由应用路由；
  * 握手信息（path/query/headers）暴露到 Session；非 WS 请求走静态/404 兜底。

  不负责：应用协议路由、业务语义、认证授权、路由/中间件。

  ## 用法

      {:ok, gw} = TroupeGateway.start_link(
        outbox: TroupeGateway.OutboxSource.ETS.new(),
        on_message: fn session, raw ->
          TroupeGateway.deliver(gw, session, %{"type" => "echo", "text" => raw})
        end,
        on_outbox: fn record ->
          TroupeGateway.broadcast(gw, record.payload, "")
        end
      )

      {:ok, port} = TroupeGateway.listen(gw, "127.0.0.1", 0)
      TroupeGateway.track(gw, "chat/room/lobby")
  """

  use GenServer

  require Logger

  alias TroupeGateway.Config
  alias TroupeGateway.Session

  @enforce_keys [:config]
  defstruct config: nil,
            outbox: nil,
            on_message: nil,
            on_connect: nil,
            on_disconnect: nil,
            on_outbox: nil,
            sessions: %{},
            tracked: MapSet.new(),
            seen_events: MapSet.new(),
            listen_socket: nil,
            acceptor: nil,
            dispatcher: nil,
            stopped: false

  @type session :: pid()
  @type payload :: String.t() | map()

  # ---- 启动 ----

  @doc """
  启动网关。

  * `:config` —— `Config` 或关键字列表
  * `:outbox` —— `TroupeGateway.OutboxSource` 实现（缺省新建 ETS 源）
  * `:on_message` —— `fn session, raw`（上行原文回调）
  * `:on_connect` —— `fn session`，false 拒绝（不触发 on_disconnect）
  * `:on_disconnect` —— `fn session`
  * `:on_outbox` —— `fn record`（去重后；raise 则不标记、下轮重投）
  """
  @spec start_link(keyword()) :: {:ok, pid()}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, nil))
  end

  @impl GenServer
  def init(opts) do
    config = opts |> Keyword.get(:config, []) |> normalize_config()

    state = %__MODULE__{
      config: config,
      outbox: Keyword.get_lazy(opts, :outbox, fn -> TroupeGateway.OutboxSource.ETS.new() end),
      on_message: Keyword.get(opts, :on_message),
      on_connect: Keyword.get(opts, :on_connect),
      on_disconnect: Keyword.get(opts, :on_disconnect),
      on_outbox: Keyword.get(opts, :on_outbox)
    }

    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  defp normalize_config(%Config{} = c), do: c
  defp normalize_config(opts) when is_list(opts), do: Config.new(opts)

  # ---- 回调读取（供 Connection/Session 模块用） ----

  @doc false
  def config(pid), do: GenServer.call(pid, :config)

  @doc false
  def on_message(pid), do: GenServer.call(pid, :on_message)

  @doc false
  def on_connect(pid), do: GenServer.call(pid, :on_connect)

  @doc false
  def on_disconnect(pid), do: GenServer.call(pid, :on_disconnect)

  @doc false
  def on_outbox(pid), do: GenServer.call(pid, :on_outbox)

  # ---- 回调 setter（对齐 Crystal 版 property 风格；闭包可引用已启动的网关 pid） ----

  @doc "设置上行原文回调 `fn session, raw`。"
  @spec set_on_message(pid(), (session(), String.t() -> any())) :: :ok
  def set_on_message(pid, fun), do: GenServer.cast(pid, {:set_cb, :on_message, fun})

  @doc "设置接纳钩子 `fn session -> boolean`（false 拒绝，不触发 on_disconnect）。"
  @spec set_on_connect(pid(), (session() -> boolean())) :: :ok
  def set_on_connect(pid, fun), do: GenServer.cast(pid, {:set_cb, :on_connect, fun})

  @doc "设置断线清理回调 `fn session`（收到的 session 为其信息快照 struct）。"
  @spec set_on_disconnect(pid(), (map() -> any())) :: :ok
  def set_on_disconnect(pid, fun), do: GenServer.cast(pid, {:set_cb, :on_disconnect, fun})

  @doc "设置 Outbox 事件回调 `fn record`（去重后；raise 则不标记、下轮重投）。"
  @spec set_on_outbox(pid(), (map() -> any())) :: :ok
  def set_on_outbox(pid, fun), do: GenServer.cast(pid, {:set_cb, :on_outbox, fun})

  # ---- actor 注册（Outbox 派发循环的轮询对象，逐 actor） ----

  @doc "注册一个参与派发轮询的 actor key（如 \"chat/room/lobby\"）。"
  @spec track(pid(), String.t()) :: :ok
  def track(pid, actor_key \\ "default"), do: GenServer.cast(pid, {:track, actor_key})

  @doc "注销。"
  @spec untrack(pid(), String.t()) :: :ok
  def untrack(pid, actor_key \\ "default"), do: GenServer.cast(pid, {:untrack, actor_key})

  @doc "当前 tracked 数量。"
  @spec tracked_size(pid()) :: non_neg_integer()
  def tracked_size(pid), do: GenServer.call(pid, :tracked_size)

  # ---- 会话注册（正常会话自动注册；供测试/应用造会话） ----

  @doc false
  def register(pid, session), do: GenServer.cast(pid, {:register, session})

  @doc "注销会话。"
  @spec unregister(pid(), String.t()) :: :ok
  def unregister(pid, session_id), do: GenServer.cast(pid, {:unregister, session_id})

  @doc "当前会话数。"
  @spec sessions_size(pid()) :: non_neg_integer()
  def sessions_size(pid), do: GenServer.call(pid, :sessions_size)

  @doc "按会话 id 查询（应用路由读取 meta/握手信息用）。"
  @spec session(pid(), String.t()) :: pid() | nil
  def session(pid, session_id), do: GenServer.call(pid, {:session, session_id})

  # ---- 启停 ----

  @doc "绑定并监听；返回 {:ok, 实际端口}（port 0 → 系统分配）。"
  @spec listen(pid(), String.t(), :inet.port_number()) ::
          {:ok, :inet.port_number()} | {:error, term()}
  def listen(pid, host \\ "127.0.0.1", port \\ 0) do
    GenServer.call(pid, {:listen, host, port})
  end

  @doc "停服停派发循环（幂等）。"
  @spec stop(pid()) :: :ok
  def stop(pid), do: GenServer.stop(pid)

  # ---- 下行投递 ----

  @doc """
  投递给会话：有界 mailbox + 限时入队，超时按溢出策略处理。返回 true 表示已入队。
  payload 为二进制直发，其余 JSON 编码。
  """
  @spec deliver(pid(), session(), payload()) :: boolean()
  def deliver(_gateway_pid, session, payload) when is_pid(session) do
    raw =
      case payload do
        raw when is_binary(raw) -> raw
        other -> JSON.encode!(other)
      end

    Session.push(session, raw)
  end

  @doc "按会话 id 投递；会话不存在返回 false。"
  @spec deliver_id(pid(), String.t(), payload()) :: boolean()
  def deliver_id(gateway_pid, session_id, payload) do
    case session(gateway_pid, session_id) do
      nil -> false
      session -> deliver(gateway_pid, session, payload)
    end
  end

  @doc "广播到全部会话（可排除一个 id）；返回入队成功的会话数。"
  @spec broadcast(pid(), payload(), String.t() | nil) :: non_neg_integer()
  def broadcast(gateway_pid, payload, except \\ nil) do
    ids = session_ids(gateway_pid)

    Enum.reduce(ids, 0, fn id, count ->
      cond do
        id == except -> count
        deliver_id(gateway_pid, id, payload) -> count + 1
        true -> count
      end
    end)
  end

  @doc "全部会话 id 快照。"
  @spec session_ids(pid()) :: [String.t()]
  def session_ids(pid), do: GenServer.call(pid, :session_ids)

  @doc "事件去重查询（至少一次 → 消费侧 effectively-once；溢出整表清空）。"
  @spec duplicate?(pid(), String.t()) :: boolean()
  def duplicate?(pid, event_id), do: GenServer.call(pid, {:duplicate?, event_id})

  # ---- GenServer ----

  @impl GenServer
  def handle_call(:config, _from, state), do: {:reply, state.config, state}
  def handle_call(:on_message, _from, state), do: {:reply, state.on_message, state}
  def handle_call(:on_connect, _from, state), do: {:reply, state.on_connect, state}
  def handle_call(:on_disconnect, _from, state), do: {:reply, state.on_disconnect, state}
  def handle_call(:on_outbox, _from, state), do: {:reply, state.on_outbox, state}
  def handle_call(:tracked_size, _from, state), do: {:reply, MapSet.size(state.tracked), state}
  def handle_call(:sessions_size, _from, state), do: {:reply, map_size(state.sessions), state}
  def handle_call(:session_ids, _from, state), do: {:reply, Map.keys(state.sessions), state}

  def handle_call({:session, id}, _from, state),
    do: {:reply, Map.get(state.sessions, id), state}

  def handle_call(:tracked_keys, _from, state),
    do: {:reply, MapSet.to_list(state.tracked), state}

  def handle_call({:duplicate?, event_id}, _from, %{config: config} = state) do
    if config.dedup do
      if MapSet.member?(state.seen_events, event_id) do
        {:reply, true, state}
      else
        seen =
          state.seen_events
          |> MapSet.put(event_id)
          |> maybe_clear(config.dedup_cache)

        {:reply, false, %{state | seen_events: seen}}
      end
    else
      {:reply, false, state}
    end
  end

  def handle_call({:listen, host, port}, _from, state) do
    with {:ok, ip} <- parse_host(host),
         {:ok, socket} <-
           :gen_tcp.listen(port, [
             :binary,
             packet: :raw,
             active: false,
             reuseaddr: true,
             backlog: 128,
             ip: ip
           ]),
         {:ok, actual} <- :inet.port(socket) do
      gateway = self()

      {:ok, acceptor} = Task.start_link(fn -> accept_loop(gateway, socket) end)

      {:ok, dispatcher} =
        Task.start_link(fn -> dispatch_loop(gateway, state.outbox, state.config) end)

      {:reply, {:ok, actual},
       %{state | listen_socket: socket, acceptor: acceptor, dispatcher: dispatcher}}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_cast({:track, key}, state),
    do: {:noreply, %{state | tracked: MapSet.put(state.tracked, key)}}

  def handle_cast({:untrack, key}, state),
    do: {:noreply, %{state | tracked: MapSet.delete(state.tracked, key)}}

  def handle_cast({:register, session_pid}, state) do
    id = Session.info(session_pid).id
    {:noreply, %{state | sessions: Map.put(state.sessions, id, session_pid)}}
  end

  def handle_cast({:unregister, id}, state),
    do: {:noreply, %{state | sessions: Map.delete(state.sessions, id)}}

  def handle_cast({:set_cb, key, fun}, state)
      when key in [:on_message, :on_connect, :on_disconnect, :on_outbox] do
    {:noreply, %{state | key => fun}}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.listen_socket, do: :gen_tcp.close(state.listen_socket)

    state.sessions
    |> Map.values()
    |> Enum.each(fn pid -> if Process.alive?(pid), do: Session.cleanup(pid) end)

    :ok
  end

  # ---- 内部 ----

  defp maybe_clear(seen, cap) do
    if MapSet.size(seen) > cap do
      # 溢出整表清空（对齐 Crystal 版）
      MapSet.new()
    else
      seen
    end
  end

  defp parse_host(host) when is_binary(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, ip} -> {:ok, ip}
      {:error, _} -> {:error, {:bad_host, host}}
    end
  end

  defp accept_loop(gateway, socket) do
    case :gen_tcp.accept(socket) do
      {:ok, conn} ->
        Task.start(fn -> TroupeGateway.Connection.serve(gateway, conn) end)
        accept_loop(gateway, socket)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Logger.warning("accept 失败：#{inspect(reason)}")
        :ok
    end
  end

  # Outbox 派发循环：tracked 快照、逐 actor 轮询；
  # 回调不 raise 才 mark_delivered（至少一次 + 消费侧去重 = effectively-once）。
  defp dispatch_loop(gateway, outbox, config) do
    loop = fn loop_fn ->
      keys = GenServer.call(gateway, :tracked_keys, 5_000)

      Enum.each(keys, fn key ->
        try do
          records = TroupeGateway.OutboxSource.poll(outbox, key, config.poll_batch)

          Enum.each(records, fn record ->
            if GenServer.call(gateway, {:duplicate?, record.event_id}, 5_000) do
              # 重复事件：照常标记（至少一次 + 去重 = effectively-once）
              TroupeGateway.OutboxSource.mark_delivered(outbox, key, record.event_id)
            else
              callback = GenServer.call(gateway, :on_outbox, 5_000)

              if callback do
                try do
                  callback.(record)
                  TroupeGateway.OutboxSource.mark_delivered(outbox, key, record.event_id)
                rescue
                  # 处理失败不标记 → 下轮重投
                  _ -> :ok
                end
              else
                TroupeGateway.OutboxSource.mark_delivered(outbox, key, record.event_id)
              end
            end
          end)
        rescue
          # 轮询失败静默，下轮重试
          _ -> :ok
        end
      end)

      Process.sleep(config.dispatch_interval)
      loop_fn.(loop_fn)
    end

    loop.(loop)
  end
end
