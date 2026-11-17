defmodule TroupeGateway.Config do
  @moduledoc """
  网关配置（ADR-0007 L3）。与 Crystal 版 Config 对齐，时长用毫秒。

  * `:mailbox_cap` —— 每会话出站 mailbox 容量（默认 256）
  * `:push_wait` —— 限时入队等待毫秒，超时视为溢出（默认 200）
  * `:overflow` —— `:drop`（丢弃该条消息、会话保留，默认）| `:disconnect`（丢弃并断开会话）
  * `:dedup` —— event_id 去重开关（默认 true）
  * `:dedup_cache` —— 去重集合容量，超出整表清空（默认 4096，与 Crystal 版一致）
  * `:dispatch_interval` —— Outbox 派发循环间隔毫秒（默认 50）
  * `:poll_batch` —— 派发循环单次 poll 的事件批量（默认 64）
  * `:service_name` —— 404 兜底应答中的服务名
  * `:static_dir` —— 可选：同端口托管静态目录（非 WS 请求命中文件回文件，否则 404 JSON）
  """

  @enforce_keys []
  defstruct mailbox_cap: 256,
            push_wait: 200,
            overflow: :drop,
            dedup: true,
            dedup_cache: 4096,
            dispatch_interval: 50,
            poll_batch: 64,
            service_name: "troupe-gateway",
            static_dir: nil

  @type t :: %__MODULE__{
          mailbox_cap: pos_integer(),
          push_wait: non_neg_integer(),
          overflow: :drop | :disconnect,
          dedup: boolean(),
          dedup_cache: pos_integer(),
          dispatch_interval: non_neg_integer(),
          poll_batch: pos_integer(),
          service_name: String.t(),
          static_dir: String.t() | nil
        }

  @doc "由关键字列表构造（未给的项取默认值）。"
  @spec new(keyword()) :: t()
  def new(opts \\ []) when is_list(opts) do
    struct!(__MODULE__, opts)
  end
end
