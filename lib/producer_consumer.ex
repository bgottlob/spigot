defmodule Spigot.ProducerConsumer do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    {:producer_consumer, MapSet.new(), dispatcher: Spigot.KeyedDispatcher}
  end

  def handle_events(events, _from, key_set) do
    key_set = Enum.reduce(events, key_set, fn {key, _data}, acc ->
      case MapSet.member?(acc, key) do
        true -> acc
        false ->
          {:ok, consumer} = Spigot.Consumer.start_link(key)
          IO.puts("Creating new consumer #{inspect(consumer)} to handle key #{key}")
          GenStage.sync_subscribe(consumer, to: self(), key: key)
          MapSet.put(acc, key)
      end
    end)
    {:noreply, events, key_set}
  end
end
