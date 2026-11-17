defmodule TroupeGateway.OutboxRecord do
  @moduledoc "Outbox 事件（消费视角）：event_id 为去重键。"

  @enforce_keys [:event_id, :topic, :payload]
  defstruct event_id: nil,
            actor_key: nil,
            topic: nil,
            payload: nil,
            created_at: nil

  @type t :: %__MODULE__{
          event_id: String.t(),
          actor_key: String.t() | nil,
          topic: String.t(),
          payload: term(),
          created_at: DateTime.t() | nil
        }
end

defmodule TroupeGateway.OutboxSource do
  @moduledoc """
  Outbox 源契约：网关派发循环对 tracked actor 逐个 `poll/3`，
  事件（至少一次）经 event_id 去重后交 `:on_outbox` 回调，成功后 `mark_delivered/3`。

  网关以 `source.__struct__` 分派 behaviour 模块；回调以 source 结构为第一参数。

  与 Crystal 版差异：troupe-elixir 核心当前没有 Outbox（见 ADR-0007 与
  troupe-planning 文档），按社区裁决本包**内置** Outbox 源——默认
  `TroupeGateway.OutboxSource.ETS`（单机内存实现，测试/示例开箱即用）；
  生产 PG 实现可实现同一 behaviour 替入（claim/lease/退避语义见 trouperb/troupego）。
  """

  alias TroupeGateway.OutboxRecord

  @callback poll(source :: term(), actor_key :: String.t(), limit :: pos_integer()) :: [
              OutboxRecord.t()
            ]

  @callback mark_delivered(source :: term(), actor_key :: String.t(), event_id :: String.t()) ::
              :ok

  @doc "按 source 结构分派 poll。"
  @spec poll(term(), String.t(), pos_integer()) :: [OutboxRecord.t()]
  def poll(source, actor_key, limit), do: module_of(source).poll(source, actor_key, limit)

  @doc "按 source 结构分派 mark_delivered。"
  @spec mark_delivered(term(), String.t(), String.t()) :: :ok
  def mark_delivered(source, actor_key, event_id),
    do: module_of(source).mark_delivered(source, actor_key, event_id)

  defp module_of(%struct{}), do: struct
  defp module_of(mod) when is_atom(mod), do: mod
end

defmodule TroupeGateway.OutboxSource.ETS do
  @moduledoc """
  内存 ETS Outbox 源：`append/4` 即产出待投递事件；`poll/3` 返回 pending
  记录（不删）；`mark_delivered/3` 删除——回调异常未标记时记录保留，
  下轮 poll 重投（至少一次）。测试/单机示例用；非跨节点安全。
  """

  alias TroupeGateway.OutboxRecord
  alias TroupeGateway.OutboxSource

  @behaviour OutboxSource

  @enforce_keys [:table]
  defstruct table: nil

  @type t :: %__MODULE__{table: :ets.tid()}

  @doc "新建源（每个实例一张私有表）。"
  @spec new() :: t()
  def new do
    %__MODULE__{table: :ets.new(:troupe_gateway_outbox, [:ordered_set, :public])}
  end

  @doc """
  追加一条事件（生产侧：应用在 actor 提交后调用）。

  * `:actor_key` —— 归属 actor（默认 "default"，对齐 Crystal track 的轮询键）
  * `:event_id` —— 业务去重键（缺省自动生成）
  """
  @spec append(t(), String.t(), term(), keyword()) :: OutboxRecord.t()
  def append(source, topic, payload, opts \\ []) do
    actor_key = Keyword.get(opts, :actor_key, "default")
    event_id = Keyword.get(opts, :event_id, default_event_id())
    created_at = DateTime.utc_now()

    record = %OutboxRecord{
      event_id: event_id,
      actor_key: actor_key,
      topic: topic,
      payload: payload,
      created_at: created_at
    }

    true =
      :ets.insert(
        source.table,
        {{actor_key, seq()}, event_id, topic, payload, :pending, created_at}
      )

    record
  end

  @impl OutboxSource
  def poll(source, actor_key, limit) do
    spec = [
      {
        # 行结构：{{actor_key, seq}, event_id, topic, payload, status, created_at}
        {{:"$1", :_}, :"$2", :"$3", :"$4", :_, :"$6"},
        [{:==, :"$1", {:const, actor_key}}],
        [[:"$2", :"$1", :"$3", :"$4", :"$6"]]
      }
    ]

    source.table
    |> :ets.select(spec)
    |> Enum.take(limit)
    |> Enum.map(fn [event_id, ak, topic, payload, created_at] ->
      %OutboxRecord{
        event_id: event_id,
        actor_key: ak,
        topic: topic,
        payload: payload,
        created_at: created_at
      }
    end)
  end

  @impl OutboxSource
  def mark_delivered(source, actor_key, event_id) do
    rows = :ets.tab2list(source.table)

    Enum.each(rows, fn
      {{ak, _seq}, eid, _topic, _payload, :pending, _ts} = row
      when eid == event_id and ak == actor_key ->
        true = :ets.delete_object(source.table, row)

      _row ->
        :ok
    end)

    :ok
  end

  defp seq, do: :erlang.unique_integer([:monotonic, :positive])

  defp default_event_id do
    :crypto.strong_rand_bytes(12) |> Base.encode16(case: :lower)
  end
end
