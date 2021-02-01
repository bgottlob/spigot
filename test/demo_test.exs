defmodule A do
  use GenStage

  def start_link(number) do
    GenStage.start_link(A, number)
  end

  def init(counter) do
    {:producer, counter}
  end

  def handle_demand(demand, counter) when demand > 0 do
    # If the counter is 3 and we ask for 2 items, we will
    # emit the items 3 and 4, and set the state to 5.
    events = Enum.map(
      counter..counter+demand-1,
      fn num when rem(num, 2) == 0 -> {"even", num}
	 num -> {"odd", num}
      end
    )
    {:noreply, events, counter + demand}
  end
end

{:ok, a} = A.start_link(0)  # starting from zero
{:ok, pc} = Spigot.ProducerConsumer.start_link() # state does not matter
{:ok, dummy} = Spigot.Consumer.start_link("fake")
IO.puts("Dummy pid #{inspect(dummy)}")

GenStage.sync_subscribe(pc, to: a)
GenStage.sync_subscribe(dummy, to: pc)

Process.sleep 5000
