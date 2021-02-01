defmodule EvenOddProducer do
  use GenStage

  def start_link(number) do
    GenStage.start_link(A, number)
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

{:ok, producer} = A.start_link(0)  # starting from zero
{:ok, spigot} = Spigot.ProducerConsumer.start_link() # state does not matter

GenStage.sync_subscribe(spigot, to: producer)

Process.sleep 5000
