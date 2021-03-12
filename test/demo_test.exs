defmodule EvenOddProducer do
  use GenStage

  def start_link(number) do
    GenStage.start_link(__MODULE__, number)
  end

  def init(counter) do
    {:producer, counter}
  end

  def handle_demand(demand, counter) when demand > 0 do
    events = Enum.map( counter..counter+demand-1,
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

  def handle_event(event, sum) do
    Process.sleep(500)
    Logger.info("#{inspect(self())} adding #{inspect(event)} for sum #{sum}")
    sum = sum + event
    {:ignore, [sum], sum}
  end
end

defmodule EvenOddTriggeredWorker do
  @behaviour Spigot.Worker

  require Logger

  def init() do
    {:ok, 0}
  end

  def handle_event(event, sum) do
    Process.sleep(500)
    case rem(event, 3) do
      0 -> {:fire_trigger, [], sum}
      _ -> {:continue, [], sum}
    end
  end

  def trigger(events, sum) do
    Logger.info("#{inspect(self())} adding #{inspect(events)} for sum #{sum}")
    sum = Enum.reduce(events, sum, fn x, acc -> x + acc end)
    {:ok, [sum], sum}
  end
end

defmodule SumSink do
  use GenStage

  require Logger

  def start_link() do
    GenStage.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    {:consumer, :no_state}
  end

  def handle_events(events, from, :no_state) do
    Logger.info("Got sums #{inspect(events)} from #{inspect(from)}")
    {:noreply, [], :no_state}
  end
end

{:ok, producer} = EvenOddProducer.start_link(0)  # starting from zero
{:ok, sink} = SumSink.start_link()
{:ok, spigot} = Spigot.ProducerConsumer.start_link(EvenOddWorker, sink)
GenStage.sync_subscribe(spigot, to: producer)

{:ok, producer_triggered} = EvenOddProducer.start_link(0)  # starting from zero
{:ok, sink_triggered} = SumSink.start_link()
{:ok, spigot_triggered} = Spigot.ProducerConsumer.start_link(EvenOddTriggeredWorker, sink_triggered)
GenStage.sync_subscribe(spigot_triggered, to: producer_triggered)

Process.sleep 5000
