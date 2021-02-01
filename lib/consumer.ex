defmodule Spigot.Consumer do
  use GenStage

  def start_link(key) do
    GenStage.start_link(__MODULE__, key)
  end

  def init(key) do
    {:consumer, key}
  end

  def handle_events(events, _from, key) do
    Process.sleep(500)
    IO.puts("#{inspect(self())} consuming #{inspect(events)}")
    {:noreply, [], key}
  end
end
