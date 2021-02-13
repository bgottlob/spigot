defmodule EvenOddProducer do
  use GenStage

  def start_link(number) do
    GenStage.start_link(__MODULE__, number)
  end

  def init(counter) do
    {:producer, counter}
  end

  def handle_demand(demand, counter) when demand > 0 do
    events = Enum.map(
      counter..counter+demand-1,
      fn num when rem(num, 2) == 0 -> {"even", num}
	 num -> {"odd", num}
      end
    )
    {:noreply, events, counter + demand}
  end
end

defmodule EvenOddWorker do
  @behaviour Spigot.Worker

  require Logger

  def init() do
    {:ok, 0}
  end

  def handle_events(events, sum) do
    Process.sleep(500)
    sum = Enum.reduce(events, sum, fn x, acc -> x + acc end)
    Logger.info("#{inspect(self())} adding #{inspect(events)} for sum #{sum}")
    {:noreply, sum}
  end
end

{:ok, producer} = EvenOddProducer.start_link(0)  # starting from zero
{:ok, spigot} = Spigot.ProducerConsumer.start_link(EvenOddWorker) # state does not matter

GenStage.sync_subscribe(spigot, to: producer)

Process.sleep 5000
