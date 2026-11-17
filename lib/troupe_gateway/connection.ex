defmodule TroupeGateway.Connection do
  @moduledoc """
  单连接处理：HTTP 升级握手 → 会话建立 → 读循环（帧解析 + 上行回调）→ 断线清理。
  非 WS 请求走静态目录/404 JSON 兜底。
  """

  alias TroupeGateway.Frame
  alias TroupeGateway.Session

  @mime %{
    ".html" => "text/html; charset=utf-8",
    ".js" => "text/javascript; charset=utf-8",
    ".css" => "text/css; charset=utf-8",
    ".svg" => "image/svg+xml",
    ".png" => "image/png"
  }

  @doc "连接入口（由 acceptor spawn）。"
  @spec serve(pid(), :gen_tcp.socket()) :: :ok
  def serve(gateway, socket) do
    case read_http_request(socket) do
      {:ok, request} ->
        if websocket_upgrade?(request) do
          serve_ws(gateway, socket, request)
        else
          fallback_http(gateway, socket, request.path)
          :gen_tcp.close(socket)
        end

      {:error, _reason} ->
        :gen_tcp.close(socket)
    end

    :ok
  end

  # ---- WebSocket 路径 ----

  defp serve_ws(gateway, socket, request) do
    accept = Frame.accept_key(request.headers["sec-websocket-key"])
    :gen_tcp.send(socket, Frame.handshake_response(accept))

    session =
      Session.start_link(
        gateway: gateway,
        path: request.path,
        query: request.query,
        headers: request.headers,
        socket: socket,
        config: TroupeGateway.config(gateway)
      )
      |> elem(1)

    if accept_session?(gateway, session) do
      TroupeGateway.register(gateway, session)
      reader_loop(gateway, socket, session)
      # 断线路径：cleanup（幂等）→ 关 mailbox。
      Session.cleanup(session)
      Session.close_mailbox(session)
    else
      # 拒接入：不注册、不触发 on_disconnect（对齐 Crystal 版非对称语义）
      :gen_tcp.send(socket, Frame.encode_close(1008, "rejected"))
      GenServer.stop(session, :normal)
    end

    :gen_tcp.close(socket)
    :ok
  end

  defp accept_session?(gateway, session) do
    hook = TroupeGateway.on_connect(gateway)

    if hook do
      hook.(session)
    else
      true
    end
  end

  # 读循环：帧解析 → 上行回调（协议路由归应用）；控制帧在此处理。
  defp reader_loop(gateway, socket, session) do
    case Frame.read(socket) do
      {:closed, _, _} ->
        :ok

      {:ok, _fin, 0x8, payload} ->
        # close：回 close 帧
        Session.try_push(session, Frame.op_close(), payload)

      {:ok, _fin, 0x9, payload} ->
        # ping → pong
        Session.try_push(session, Frame.op_pong(), payload)
        reader_loop(gateway, socket, session)

      {:ok, _fin, 0xA, _payload} ->
        reader_loop(gateway, socket, session)

      {:ok, true, 0x1, payload} ->
        deliver_upstream(gateway, session, payload)
        reader_loop(gateway, socket, session)

      {:ok, _fin, 0x1, _payload} ->
        # 分片首帧：测试矩阵不依赖，续帧按整帧丢弃语义对齐 Crystal stdlib
        reader_loop(gateway, socket, session)

      {:ok, _fin, 0x0, _payload} ->
        reader_loop(gateway, socket, session)

      {:ok, _fin, 0x2, _payload} ->
        raise Frame.ProtocolError, "二进制帧不受支持（协议为文本帧 JSON）"
    end
  rescue
    Frame.ProtocolError ->
      # 协议违规：关停连接（best-effort close 帧由 cleanup 发送）
      :ok
  end

  defp deliver_upstream(gateway, session, payload) do
    callback = TroupeGateway.on_message(gateway)

    if callback do
      callback.(session, payload)
    end
  end

  # ---- HTTP 路径 ----

  # 非 WebSocket 请求的兜底应答：配置 static_dir 时尝试回静态文件，否则 404 JSON。
  defp fallback_http(gateway, socket, raw_path) do
    config = TroupeGateway.config(gateway)
    path = raw_path |> String.split("?", parts: 2) |> List.first() || "/"
    file = if path == "/", do: "index.html", else: String.trim_leading(path, "/")

    served =
      with dir when is_binary(dir) <- config.static_dir,
           false <- String.contains?(file, ".."),
           full = Path.join(dir, file),
           {:ok, %File.Stat{type: :regular}} <- File.stat(full),
           {:ok, body} <- File.read(full) do
        content_type = Map.get(@mime, Path.extname(full), "application/octet-stream")
        write_http(socket, "200 OK", content_type, body)
        true
      else
        _ -> false
      end

    unless served do
      body = JSON.encode!(%{service: config.service_name})
      write_http(socket, "404 Not Found", "application/json", body)
    end

    :ok
  end

  defp write_http(socket, status, content_type, body) do
    :gen_tcp.send(socket, [
      "HTTP/1.1 ",
      status,
      "\r\n",
      "Content-Type: ",
      content_type,
      "\r\n",
      "Content-Length: ",
      Integer.to_string(byte_size(body)),
      "\r\n",
      "Connection: close\r\n\r\n",
      body
    ])
  end

  # ---- HTTP 请求解析 ----

  defp read_http_request(socket) do
    with {:ok, head} <- recv_head(socket),
         [request_line | header_lines] <- String.split(head, "\r\n") do
      parts = String.split(request_line, " ")

      case parts do
        ["GET", path, _version | _] ->
          headers = parse_headers(header_lines, %{})
          query = parse_query(path)

          {:ok,
           %{
             path: path |> String.split("?", parts: 2) |> List.first() || "/",
             query: query,
             headers: headers
           }}

        _ ->
          {:error, :bad_request}
      end
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :bad_request}
    end
  end

  defp recv_head(socket, acc \\ "") do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} ->
        acc = acc <> data

        if String.contains?(acc, "\r\n\r\n") do
          [head, _rest] = String.split(acc, "\r\n\r\n", parts: 2)
          {:ok, head}
        else
          recv_head(socket, acc)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_headers([], acc), do: acc

  defp parse_headers([line | rest], acc) do
    case String.split(line, ":", parts: 2) do
      [k, v] -> parse_headers(rest, Map.put(acc, String.downcase(String.trim(k)), String.trim(v)))
      _ -> parse_headers(rest, acc)
    end
  end

  defp parse_query(path) do
    case String.split(path, "?", parts: 2) do
      [_, qs] when qs != "" ->
        qs
        |> String.split("&")
        |> Enum.reduce(%{}, fn pair, acc ->
          case String.split(pair, "=", parts: 2) do
            [k, v] -> Map.put(acc, URI.decode_www_form(k), URI.decode_www_form(v))
            [k] -> Map.put(acc, URI.decode_www_form(k), "")
            _ -> acc
          end
        end)

      _ ->
        %{}
    end
  end

  defp websocket_upgrade?(request) do
    headers = request.headers

    String.downcase(Map.get(headers, "upgrade", "")) == "websocket" and
      Map.get(headers, "sec-websocket-version") == "13" and
      not (Map.get(headers, "sec-websocket-key", "") == "")
  end
end
