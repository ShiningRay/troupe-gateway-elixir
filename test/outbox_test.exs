defmodule TroupeGateway.OutboxTest do
  @moduledoc """
  Outbox 派发循环（移植 troupe-gateway Crystal spec/outbox_dispatch_spec.cr，
  以内置 ETS OutboxSource 模拟事件源）。
  """

  use ExUnit.Case, async: false

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

  test "track 的 actor 事件经派发循环送达 on_outbox" do
    source = TroupeGateway.OutboxSource.ETS.new()
    test_pid = self()

    {:ok, gw} = TroupeGateway.start_link(outbox: source, config: [dispatch_interval: 25])

    TroupeGateway.set_on_outbox(gw, fn record -> send(test_pid, {:record, record}) end)

    {:ok, _port} = TroupeGateway.listen(gw, "127.0.0.1", 0)
    on_exit(fn -> safe_stop(gw) end)

    TroupeGateway.track(gw, "gw/emitter/1")
    assert TroupeGateway.tracked_size(gw) == 1

    TroupeGateway.OutboxSource.ETS.append(source, "ping", %{"n" => 1},
      event_id: "e-1",
      actor_key: "gw/emitter/1"
    )

    assert_receive {:record, record}, 3_000
    assert record.topic == "ping"
    assert record.payload == %{"n" => 1}
    assert record.event_id == "e-1"

    TroupeGateway.untrack(gw, "gw/emitter/1")
    assert TroupeGateway.tracked_size(gw) == 0
  end

  test "未 track 不派发；track 后送达且同 event_id 去重" do
    source = TroupeGateway.OutboxSource.ETS.new()
    test_pid = self()

    {:ok, gw} = TroupeGateway.start_link(outbox: source, config: [dispatch_interval: 25])

    TroupeGateway.set_on_outbox(gw, fn record -> send(test_pid, {:record, record.event_id}) end)

    {:ok, _port} = TroupeGateway.listen(gw, "127.0.0.1", 0)
    on_exit(fn -> safe_stop(gw) end)

    # 未 track：不派发
    TroupeGateway.OutboxSource.ETS.append(source, "quiet", %{}, event_id: "q-1")
    Process.sleep(150)
    refute_receive {:record, "q-1"}

    # track 后：送达
    TroupeGateway.track(gw)
    assert_receive {:record, "q-1"}, 3_000

    # 同 event_id 的重复记录只回调一次（去重吸收，但照常标记完成）
    TroupeGateway.OutboxSource.ETS.append(source, "ping", %{}, event_id: "dup-1")
    TroupeGateway.OutboxSource.ETS.append(source, "ping", %{}, event_id: "dup-1")
    assert_receive {:record, "dup-1"}, 3_000
    Process.sleep(150)
    refute_receive {:record, "dup-1"}
  end

  test "回调失败不标记：pending 保留、去重吸收后下轮标记（effectively-once）" do
    source = TroupeGateway.OutboxSource.ETS.new()
    test_pid = self()

    {:ok, gw} =
      TroupeGateway.start_link(
        outbox: source,
        config: [dispatch_interval: 25],
        # 处理 "boom" 事件时抛错
        on_outbox: fn record ->
          send(test_pid, {:record, record.event_id})

          if record.event_id == "boom" do
            raise "处理失败"
          end
        end
      )

    {:ok, _port} = TroupeGateway.listen(gw, "127.0.0.1", 0)
    on_exit(fn -> safe_stop(gw) end)
    TroupeGateway.track(gw)

    TroupeGateway.OutboxSource.ETS.append(source, "ok", %{}, event_id: "ok-1")
    TroupeGateway.OutboxSource.ETS.append(source, "boom", %{}, event_id: "boom")

    # boom 首次投递失败（回调异常）→ 不标记 → 下轮重投 → 去重吸收 → 照常标记
    wait_until(fn ->
      TroupeGateway.OutboxSource.ETS.poll(source, "default", 100) == []
    end)

    # on_outbox 收到过 boom 一次（重投被去重吸收，不重复回调）
    Process.sleep(150)

    ids =
      Enum.reduce_while(0..10, [], fn _, acc ->
        receive do
          {:record, eid} -> {:cont, acc ++ [eid]}
        after
          0 -> {:halt, acc}
        end
      end)

    assert Enum.count(ids, &(&1 == "boom")) == 1
    assert "ok-1" in ids
  end
end
