defmodule TroupeGateway.Frame do
  @moduledoc """
  RFC 6455 帧编解码（文本帧子集，零依赖）：
  - 服务端帧不掩码；客户端帧必须掩码（协议要求）
  - 分片（continuation）、ping/pong、close
  - 不支持二进制/扩展（网关仅 JSON 文本）
  """

  @ws_guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  @max_message 1_048_576

  import Bitwise

  alias __MODULE__.ProtocolError

  @op_cont 0x0
  @op_text 0x1
  @op_close 0x8
  @op_ping 0x9
  @op_pong 0xA

  @type opcode :: 0x0 | 0x1 | 0x2 | 0x8 | 0x9 | 0xA

  @doc "Sec-WebSocket-Accept 值。"
  @spec accept_key(String.t()) :: String.t()
  def accept_key(key) do
    :crypto.hash(:sha, key <> @ws_guid) |> Base.encode64()
  end

  @doc "101 升级握手应答。"
  @spec handshake_response(String.t()) :: iodata()
  def handshake_response(accept) do
    [
      "HTTP/1.1 101 Switching Protocols\r\n",
      "Upgrade: websocket\r\n",
      "Connection: Upgrade\r\n",
      "Sec-WebSocket-Accept: ",
      accept,
      "\r\n\r\n"
    ]
  end

  @doc "编码服务端帧（不掩码）。"
  @spec encode(opcode(), iodata() | binary()) :: iodata()
  def encode(op, payload) do
    len = IO.iodata_length(payload)

    header =
      cond do
        len < 126 -> [0x80 ||| op, len]
        len < 65_536 -> [0x80 ||| op, 126, <<len::16>>]
        true -> [0x80 ||| op, 127, <<len::64>>]
      end

    [header, payload]
  end

  @doc "编码 close 帧（code + reason）。"
  @spec encode_close(pos_integer(), String.t()) :: iodata()
  def encode_close(code, reason \\ "") do
    encode(@op_close, <<code::16, reason::binary>>)
  end

  @doc "掩码载荷（测试客户端用）。"
  @spec mask_payload(binary(), binary()) :: binary()
  def mask_payload(payload, mask) do
    payload
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {b, i} -> :erlang.bxor(b, :binary.at(mask, rem(i, 4))) end)
    |> :binary.list_to_bin()
  end

  @doc """
  解析一帧（服务端被动 socket）。返回 `{:ok, fin, op, payload}`；
  连接关闭返回 `{:closed, nil, nil}`；协议违规抛 `ProtocolError`。
  """
  @spec read(:gen_tcp.socket()) :: {:ok, boolean(), opcode(), binary()} | {:closed, nil, nil}
  def read(socket) do
    with {:ok, <<b0, b1>>} <- recv_exact(socket, 2),
         fin <- :erlang.band(b0, 0x80) != 0,
         op <- :erlang.band(b0, 0x0F),
         masked <- :erlang.band(b1, 0x80) != 0,
         len0 <- :erlang.band(b1, 0x7F),
         {:ok, len} <- read_length(socket, len0),
         :ok <- check_length(op, len),
         {:ok, mask} <- read_mask(socket, masked),
         {:ok, payload} <- recv_exact(socket, len) do
      payload =
        if masked and len > 0 do
          mask_payload(payload, mask)
        else
          payload
        end

      if not masked and op < 0x8 do
        raise ProtocolError, "客户端数据帧未掩码"
      end

      {:ok, fin, op, payload}
    else
      {:error, :closed} -> {:closed, nil, nil}
      {:error, reason} -> raise ProtocolError, "读帧失败：#{inspect(reason)}"
    end
  end

  @doc "消息超长上限。"
  def max_message, do: @max_message

  @doc "文本帧 opcode。"
  def op_text, do: @op_text
  @doc "close 帧 opcode。"
  def op_close, do: @op_close
  @doc "ping 帧 opcode。"
  def op_ping, do: @op_ping
  @doc "pong 帧 opcode。"
  def op_pong, do: @op_pong
  @doc "continuation 帧 opcode。"
  def op_cont, do: @op_cont

  defmodule ProtocolError do
    defexception [:message]
  end

  # ---- 内部 ----

  defp read_length(socket, 126), do: recv_exact(socket, 2) |> map_len(16)
  defp read_length(socket, 127), do: recv_exact(socket, 8) |> map_len(64)
  defp read_length(_socket, len), do: {:ok, len}

  defp map_len({:ok, bin}, 16), do: {:ok, :binary.decode_unsigned(bin)}
  defp map_len({:ok, bin}, 64), do: {:ok, :binary.decode_unsigned(bin)}
  defp map_len({:error, _} = e, _), do: e

  defp check_length(op, len) when op >= 0x8 do
    if len > 125, do: raise(ProtocolError, "控制帧负载超过 125 字节")
    :ok
  end

  defp check_length(_op, len) do
    if len > @max_message, do: raise(ProtocolError, "消息超长（>#{@max_message} 字节）")
    :ok
  end

  defp read_mask(_socket, false), do: {:ok, nil}
  defp read_mask(socket, true), do: recv_exact(socket, 4)

  defp recv_exact(_socket, 0), do: {:ok, <<>>}

  defp recv_exact(socket, n) do
    case :gen_tcp.recv(socket, n) do
      {:ok, data} when byte_size(data) == n -> {:ok, data}
      {:ok, data} -> recv_more(socket, n - byte_size(data), data)
      {:error, _} = e -> e
    end
  end

  defp recv_more(_socket, 0, acc), do: {:ok, acc}

  defp recv_more(socket, n, acc) do
    case :gen_tcp.recv(socket, n) do
      {:ok, data} -> recv_more(socket, n - byte_size(data), acc <> data)
      {:error, _} = e -> e
    end
  end
end
