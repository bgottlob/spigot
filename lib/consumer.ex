defmodule Spigot.Consumer do
  use GenStage

  def start_link(mod) do
    GenStage.start_link(__MODULE__, mod)
  end

  def init(mod) do
    {:ok, state} = mod.init()
    {:consumer, {mod, state}}
  end

  def handle_events(events, _from, {mod, state}) do
    mod.handle_events(events, state)
    {:noreply, [], {mod, state}}
  end
end
