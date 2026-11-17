defmodule TroupeGateway.Session do
  @moduledoc """
  会话：一条外部连接在网关内的表示（GenServer）。

  持有出站有界 mailbox 与 socket 写权（单进程写，免锁）；容量/溢出策略由网关配置驱动：
  `push/4` 容量满时按 `:push_wait` 限时等待——等待方经 `handle_call` 延迟应答实现
  （对齐 Crystal 版 `select`/`timeout` 语义），超时按溢出策略处理。
  """

  use GenServer

  alias TroupeGateway.Config
  alias TroupeGateway.Frame

  @enforce_keys [:id, :gateway]
  defstruct id: nil,
            gateway: nil,
            meta: %{},
            path: "",
            query: %{},
            headers: %{},
            socket: nil,
            mailbox: :queue.new(),
            waiters: [],
            closed: false

  @type t :: %__MODULE__{
          id: String.t(),
          gateway: pid() | nil,
          meta: %{optional(String.t()) => String.t()},
          path: String.t(),
          query: %{optional(String.t()) => String.t()},
          headers: map(),
          socket: :gen_tcp.socket() | nil,
          mailbox: :queue.queue(String.t()),
          waiters: list(),
          closed: boolean()
        }

  # ---- 启动 ----

  @doc """
  启动会话。

  * `:id` —— 会话 id（缺省 16 位 hex 随机）
  * `:gateway` —— 所属网关 pid（nil = 独立测试会话）
  * `:path/:query/:headers` —— 握手信息
  * `:socket` —— TCP socket（nil = 无真实连接，如溢出策略测试）
  * `:config` —— `TroupeGateway.Config`
  """
  @spec start_link(keyword()) :: {:ok, pid()}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # ---- API ----

  @doc "限时入队：容量满时最多等本会话配置的 `push_wait` ms，超时后本条不再入队（返回 false）。"
  @spec push(pid(), String.t()) :: boolean()
  def push(pid, raw) do
    config = GenServer.call(pid, :get_config)
    call_timeout = max(config.push_wait, 0) + 5_000

    try do
      GenServer.call(pid, {:push, raw}, call_timeout)
    catch
      :exit, {:timeout, _} -> false
      :exit, _ -> false
    end
  end

  @doc "非阻塞控制帧入队（满/关即丢）：pong/close 用。"
  @spec try_push(pid(), Frame.opcode(), binary()) :: :ok
  def try_push(pid, op, payload) do
    GenServer.cast(pid, {:try_push, op, payload})
  end

  @doc "关闭 mailbox（写循环不再出队；对齐 Crystal 断线路径的第二步）。"
  @spec close_mailbox(pid()) :: :ok
  def close_mailbox(pid) do
    GenServer.cast(pid, :close_mailbox)
  end

  @doc "mailbox 是否已关闭。"
  @spec closed?(pid()) :: boolean()
  def closed?(pid) do
    GenServer.call(pid, :closed?)
  end

  @doc "幂等清理：注销 + on_disconnect（恰一次）。溢出/断线两路径共用。"
  @spec cleanup(pid()) :: :ok
  def cleanup(pid) do
    GenServer.cast(pid, :cleanup)
  end

  # ---- GenServer ----

  @impl GenServer
  def init(opts) do
    config = Keyword.get(opts, :config, Config.new())

    state = %__MODULE__{
      id: Keyword.get_lazy(opts, :id, fn -> random_id() end),
      gateway: Keyword.get(opts, :gateway),
      path: Keyword.get(opts, :path, ""),
      query: Keyword.get(opts, :query, %{}),
      headers: Keyword.get(opts, :headers, %{}),
      socket: Keyword.get(opts, :socket)
    }

    {:ok, %{state: state, config: config, cleaned: false}}
  end

  @impl GenServer
  def handle_call({:push, raw}, from, %{state: state, config: config} = st) do
    cond do
      state.closed ->
        {:reply, false, st}

      :queue.len(state.mailbox) < config.mailbox_cap ->
        {:reply, true, put_queue(st, raw)}

      true ->
        ref = make_ref()
        timer = Process.send_after(self(), {:push_timeout, ref, from}, config.push_wait)
        {:noreply, %{st | state: %{state | waiters: [{ref, from, raw, timer} | state.waiters]}}}
    end
  end

  def handle_call(:closed?, _from, %{state: state} = st) do
    {:reply, state.closed, st}
  end

  def handle_call(:get_config, _from, %{config: config} = st) do
    {:reply, config, st}
  end

  # 会话信息快照（网关注册表用；on_disconnect 回调也以此形态收到会话）
  def handle_call(:info, _from, %{state: state} = st) do
    {:reply, state, st}
  end

  # 会话信息快照（网关注册表用；on_disconnect 回调也以此形态收到会话）。
  # pid → GenServer 快照；struct（on_disconnect 已收快照）→ 原样返回。
  @spec info(pid() | t()) :: t()
  def info(pid) when is_pid(pid), do: GenServer.call(pid, :info)
  def info(%__MODULE__{} = session), do: session

  @impl GenServer
  def handle_cast({:try_push, op, payload}, %{state: state} = st) do
    unless state.closed do
      write_socket(st, Frame.encode(op, payload))
    end

    {:noreply, st}
  end

  def handle_cast(:close_mailbox, %{state: state} = st) do
    {:noreply, %{st | state: %{state | closed: true}}}
  end

  def handle_cast(:cleanup, st) do
    {:noreply, do_cleanup(st)}
  end

  @impl GenServer
  def handle_info({:push_timeout, ref, from}, %{state: state} = st) do
    case List.keyfind(state.waiters, ref, 0) do
      {^ref, ^from, _raw, _timer} ->
        waiters = List.keydelete(state.waiters, ref, 0)
        GenServer.reply(from, false)
        st = %{st | state: %{state | waiters: waiters}}
        # 超时即溢出：按策略处理（Drop 丢弃该条；Disconnect 踢除会话）
        {:noreply, overflow(st)}

      nil ->
        {:noreply, st}
    end
  end

  def handle_info({:tcp_closed, _socket}, st), do: {:noreply, st}
  def handle_info({:tcp_error, _socket, _reason}, st), do: {:noreply, st}
  def handle_info(_, st), do: {:noreply, st}

  # ---- 内部 ----

  defp put_queue(%{state: state} = st, raw) do
    st = %{st | state: %{state | mailbox: :queue.in(raw, state.mailbox)}}
    drain(st)
  end

  # 出队写 socket（无 socket 的测试会话不出队——mailbox 保持满载语义）；
  # 每让出一个空位，唤醒最早的限时等待者（同步入队+应答）。
  defp drain(%{state: %{socket: nil}} = st), do: st

  defp drain(%{state: state} = st) do
    case :queue.out(state.mailbox) do
      {{:value, raw}, rest} ->
        write_socket(st, Frame.encode(Frame.op_text(), raw))
        st = %{st | state: %{state | mailbox: rest}}

        case pop_waiter(state.waiters) do
          nil ->
            drain(st)

          {{from, waiter_raw, timer}, waiters} ->
            Process.cancel_timer(timer)
            state2 = %{st.state | waiters: waiters}
            GenServer.reply(from, true)
            drain(%{st | state: %{state2 | mailbox: :queue.in(waiter_raw, state2.mailbox)}})
        end

      {:empty, _} ->
        st
    end
  end

  defp pop_waiter([]), do: nil

  # 等待者新者在前；取最旧者（等待最久）
  defp pop_waiter(waiters) do
    case List.pop_at(waiters, -1) do
      {{_ref, from, raw, timer}, rest} -> {{from, raw, timer}, rest}
      nil -> nil
    end
  end

  defp overflow(%{config: config} = st) do
    if config.overflow == :disconnect do
      do_cleanup(st)
    else
      st
    end
  end

  defp do_cleanup(%{state: %__MODULE__{} = state, cleaned: cleaned} = st) do
    if cleaned do
      st
    else
      st = %{st | cleaned: true, state: %{state | closed: true}}

      if state.gateway && Process.alive?(state.gateway) do
        TroupeGateway.unregister(state.gateway, state.id)
        callback = TroupeGateway.on_disconnect(state.gateway)

        if callback, do: callback.(%__MODULE__{state | closed: true})
      end

      if state.socket do
        write_socket(st, Frame.encode_close(1008, "closed"))
        :gen_tcp.close(state.socket)
      end

      st
    end
  end

  defp write_socket(%{state: state}, iodata) do
    if state.socket do
      case :gen_tcp.send(state.socket, iodata) do
        :ok -> :ok
        # 写失败由读循环侧收尾
        {:error, _} -> :ok
      end
    end

    :ok
  end

  defp random_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
