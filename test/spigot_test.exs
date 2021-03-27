defmodule SpigotTest do
  use ExUnit.Case
  doctest Spigot

  defmodule ForwarderWorker do
    def init() do
      {:ok, nil}
    end

    def handle_event(event, nil) do
      {:ignore, [event], nil}
    end
  end

  defmodule StageKeySink do
    use GenStage

    require Logger

    def start_link(watcher_pid, partitioner) do
      GenStage.start_link(__MODULE__, {watcher_pid, partitioner})
    end

    def init({watcher_pid, partitioner}) do
      {:consumer, {watcher_pid, partitioner, %{}}}
    end

    def handle_events(events, {from_pid, _}, {watcher_pid, partitioner, stage_map}) do
      stage_map =
        events
        |> Enum.reduce(stage_map, fn event, acc ->
          key = partitioner.(event)
          Map.update(acc, from_pid, [key], fn key_list ->
            Enum.uniq([key | key_list])
          end)
        end)
      send(watcher_pid, Map.to_list(stage_map))
      {:noreply, [], {watcher_pid, partitioner, stage_map}}
    end
  end

  test "Evens and odds are split properly" do
    {:ok, producer} = GenStage.from_enumerable(1..5, demand: :forward)
    partitioner = fn
      event when rem(event, 2) == 0 -> "even"
      _                             -> "odd"
    end
    {:ok, sink} = StageKeySink.start_link(self(), partitioner)

    Spigot.from_stages([producer])
    |> Spigot.partition(partitioner)
    |> Spigot.worker_module(ForwarderWorker)
    |> Spigot.into_stages([sink])

    assert_receive [{_pid1, [_key1]}, {_pid2, [_key2]}]
  end
end
