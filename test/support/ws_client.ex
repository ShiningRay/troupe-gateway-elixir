defmodule TroupeGateway.WsTestClient do
  @moduledoc """
  测试客户端：手写最小 RFC 6455 客户端（:gen_tcp + 掩码帧），
  支持自定义握手头（测试矩阵需要 X-Test 头）。
  客户端进程持有接收缓冲区，逐字节解析服务端帧（不阻塞跨帧）。
  """

  defstruct [:socket, buf: <<>>, queue: [], closed: false]

  import Bitwise

  @op_text 0x1
  @op_close 0x8

  @spec connect(String.t(), map()) :: {:ok, pid()} | {:error, term()}
  def connect(url, headers \\ %{}) do
    uri = URI.parse(url)
    host = to_charlist(uri.host)
    port = uri.port || 80

    with {:ok, socket} <- :gen_tcp.connect(host, port, [:binary, active: false], 3_000),
         key = :crypto.strong_rand_bytes(16) |> Base.encode64(),
         :ok <-
           :gen_tcp.send(socket, [
             "GET #{uri.path || "/"}#{if uri.query, do: "?" <> uri.query, else: ""} HTTP/1.1\r\n",
             "Host: #{uri.host}:#{port}\r\n",
             "Upgrade: websocket\r\n",
             "Connection: Upgrade\r\n",
             "Sec-WebSocket-Key: #{key}\r\n",
             "Sec-WebSocket-Version: 13\r\n",
             Enum.map(headers, fn {k, v} -> "#{k}: #{v}\r\n" end),
             "\r\n"
           ]),
         {:ok, head} <- recv_handshake(socket) do
      if String.contains?(head, "101") do
        {:ok, spawn_link(fn -> read_loop(%__MODULE__{socket: socket}) end)}
      else
        {:error, {:upgrade_failed, String.trim(head)}}
      end
    end
  end

  defp recv_handshake(socket, acc \\ "") do
    case :gen_tcp.recv(socket, 0, 3_000) do
      {:ok, data} ->
        acc = acc <> data

        if String.contains?(acc, "\r\n\r\n") do
          [head, _rest] = String.split(acc, "\r\n\r\n", parts: 2)
          {:ok, head}
        else
          recv_handshake(socket, acc)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "发送原文（掩码文本帧）。"
  @spec send_raw(pid(), String.t()) :: :ok
  def send_raw(client_pid, raw) do
    send(client_pid, {:send_raw, raw})
    :ok
  end

  @doc "关闭连接（发 close 帧后断开）。"
  @spec close(pid()) :: :ok
  def close(client_pid) do
    send(client_pid, :close)
    :ok
  end

  @doc "等待第一条满足条件的消息（JSON 解析；不匹配的按到达顺序丢弃），超时抛错。"
  @spec expect(pid(), non_neg_integer(), (map() -> boolean())) :: map()
  def expect(client_pid, timeout \\ 3_000, pred) do
    deadline = System.monotonic_time(:millisecond) + timeout

    loop = fn loop_fn ->
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        raise "等待消息超时"
      end

      case next_raw(client_pid, min(remaining, 100)) do
        {:ok, raw} ->
          msg = JSON.decode!(raw)

          if pred.(msg) do
            msg
          else
            loop_fn.(loop_fn)
          end

        :closed ->
          raise "连接已关闭"

        :timeout ->
          loop_fn.(loop_fn)
      end
    end

    loop.(loop)
  end

  @doc "下一条原始消息；超时返回 :timeout，关闭返回 :closed。"
  @spec next_raw(pid(), non_neg_integer()) :: {:ok, String.t()} | :closed | :timeout
  def next_raw(client_pid, timeout) do
    ref = make_ref()
    send(client_pid, {:next_raw, self(), ref, timeout})

    receive do
      {^ref, {:raw, raw}} -> {:ok, raw}
      {^ref, :client_closed} -> :closed
      {^ref, :client_timeout} -> :timeout
    after
      timeout + 2_000 -> :timeout
    end
  end

  # ---- 客户端进程 ----

  defp read_loop(%__MODULE__{} = st) do
    receive do
      {:send_raw, raw} ->
        mask = :crypto.strong_rand_bytes(4)
        :ok = :gen_tcp.send(st.socket, encode_masked(@op_text, raw, mask))
        read_loop(st)

      :close ->
        mask = :crypto.strong_rand_bytes(4)
        _ = :gen_tcp.send(st.socket, encode_masked(@op_close, <<1000::16>>, mask))
        :gen_tcp.close(st.socket)
        :ok

      {:next_raw, from, ref, timeout} ->
        deadline = System.monotonic_time(:millisecond) + timeout
        {reply, st2} = take_or_read(st, deadline)
        send(from, {ref, reply})
        read_loop(st2)

      _ ->
        read_loop(st)
    end
  end

  defp take_or_read(%__MODULE__{queue: [raw | rest]} = st, _deadline) do
    {{:raw, raw}, %{st | queue: rest}}
  end

  defp take_or_read(%__MODULE__{socket: nil} = st, _deadline), do: {:client_closed, st}

  defp take_or_read(%__MODULE__{closed: true} = st, _deadline), do: {:client_closed, st}

  defp take_or_read(st, deadline) do
    case pump(st, deadline) do
      {:ok, st2} -> take_or_read(st2, deadline)
      {:closed, st2} -> {:client_closed, st2}
      {:timeout, st2} -> {:client_timeout, st2}
    end
  end

  # 尽力从 socket 收数据并解析；队列出货返回 {:ok, st}，否则到截止期返回 :timeout
  defp pump(%__MODULE__{socket: socket} = st, deadline) do
    case parse_buffer(st) do
      {:message, st2} ->
        {:ok, st2}

      {:closed, st2} ->
        {:closed, st2}

      :incomplete ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          {:timeout, st}
        else
          case :gen_tcp.recv(socket, 0, min(remaining, 20)) do
            {:ok, data} -> pump(%{st | buf: st.buf <> data}, deadline)
            {:error, :timeout} -> pump(st, deadline)
            {:error, _} -> {:closed, %{st | socket: nil, closed: true}}
          end
        end
    end
  end

  # 从缓冲区解析一帧；服务端帧不掩码。不完整时保留原缓冲区返回 :incomplete。
  defp parse_buffer(%__MODULE__{buf: buf} = st) do
    result =
      try do
        frame(buf)
      catch
        :throw, :incomplete_frame -> :incomplete
      end

    case result do
      :incomplete ->
        :incomplete

      {:closed, rest} ->
        {:closed, %{st | buf: rest, closed: true, socket: nil}}

      {op, payload, rest} ->
        st2 = %{st | buf: rest}

        case op do
          @op_text -> {:message, %{st2 | queue: st2.queue ++ [payload]}}
          @op_close -> {:closed, %{st2 | closed: true, socket: nil}}
          # ping/pong 等忽略
          _ -> parse_buffer(st2)
        end
    end
  end

  defp frame(buf) when byte_size(buf) < 2, do: :incomplete

  defp frame(<<b0, b1, rest::binary>>) do
    op = b0 &&& 0x0F
    len0 = b1 &&& 0x7F

    {len, rest} =
      cond do
        len0 == 126 ->
          if byte_size(rest) < 2, do: throw(:incomplete_frame)

          <<l::16, r::binary>> = rest
          {l, r}

        len0 == 127 ->
          if byte_size(rest) < 8, do: throw(:incomplete_frame)

          <<l::64, r::binary>> = rest
          {l, r}

        true ->
          {len0, rest}
      end

    if byte_size(rest) < len, do: throw(:incomplete_frame)

    <<payload::binary-size(^len), rest2::binary>> = rest

    cond do
      op == @op_close -> {:closed, rest2}
      true -> {op, payload, rest2}
    end
  end

  defp encode_masked(op, payload, mask) do
    len = byte_size(payload)

    masked =
      payload
      |> :binary.bin_to_list()
      |> Enum.with_index()
      |> Enum.map(fn {b, i} -> :erlang.bxor(b, :binary.at(mask, rem(i, 4))) end)
      |> :binary.list_to_bin()

    header =
      cond do
        len < 126 -> [0x80 ||| op, 0x80 ||| len]
        len < 65_536 -> [0x80 ||| op, 0x80 ||| 126, <<len::16>>]
        true -> [0x80 ||| op, 0x80 ||| 127, <<len::64>>]
      end

    IO.iodata_to_binary([header, mask, masked])
  end
end
