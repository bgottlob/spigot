defmodule SpigotTest do
  use ExUnit.Case

  defmodule OneTimeProducer do
    use GenStage

    def start_link() do
      GenStage.start_link(__MODULE__, :ok)
    end

    def init(:ok) do
      {:producer, :ok}
    end

    def handle_demand(_demand, :done) do
      {:stop, "test is complete", :ok}
    end

    def handle_demand(demand, :ok) when demand > 0 do
      {:noreply, [{"odd", 1}, {"even", 2}], :done}
    end
  end

  # A spigot worker that forwards the events for a given key to a sink process
  defmodule ForwarderWorker do
    def init() do
      {:ok, :ok}
    end

    def handle_events(events, :ok) do
      pid = Process.whereis(:sink)
      send(pid, {self(), events})
      {:noreply, :ok}
    end
  end

  describe "basic test" do
    test "basic test" do
      Process.register(self(), :sink)
      {:ok, producer} = OneTimeProducer.start_link()
      {:ok, spigot} = Spigot.ProducerConsumer.start_link(ForwarderWorker)
      GenStage.sync_subscribe(spigot, to: producer)
      receive do
	{pid, events} ->
	  IO.puts(inspect(pid))
	  IO.puts(inspect(events))
      end
    end
  end
end
