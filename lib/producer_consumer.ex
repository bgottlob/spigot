defmodule Spigot.ProducerConsumer do
  use GenStage

  require Logger

  def start_link(worker_mod, partitioner, sink) do
    {:ok, pc} = GenStage.start_link(__MODULE__, {worker_mod, partitioner, sink})

    # In order to start handling events, a producer[-consumer] must have at
    # least one consumer subscribed to it. This consumer triggers event handling
    # and will not be dispatched any events.
    {:ok, primer} = Spigot.Consumer.start_link(Spigot.PrimerWorker)
    GenStage.sync_subscribe(primer, to: pc, cancel: :transient)

    {:ok, pc}
  end

  def init({worker_mod, partitioner, sink}) do
    {:producer_consumer, {MapSet.new(), worker_mod, partitioner, sink}, dispatcher: Spigot.KeyedDispatcher}
  end

  def handle_events(events, _from, {key_set, worker_mod, partitioner, sink}) do
    events = Enum.map(events, fn e -> {partitioner.(e), e} end)
    key_set = Enum.reduce(events, key_set, fn {key, _data}, acc ->
      case MapSet.member?(acc, key) do
        true -> acc
        false ->
          {:ok, consumer} = Spigot.Consumer.start_link(worker_mod)
          Logger.debug("Creating new #{worker_mod} #{inspect(consumer)} to handle key #{key}")
          GenStage.sync_subscribe(consumer, to: self(), key: key, cancel: :transient)
          GenStage.sync_subscribe(sink, to: consumer, max_demand: 5, cancel: :transient)
          MapSet.put(acc, key)
      end
    end)
    {:noreply, events, {key_set, worker_mod, partitioner, sink}}
  end
end
