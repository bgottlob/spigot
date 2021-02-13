defmodule Spigot.Worker do
  @callback init() :: {:ok, state :: term}

  @callback handle_events(events :: [event :: term], state :: term) ::
    {:noreply, new_state :: term}
end

defmodule Spigot.PrimerWorker do
  require Logger

  def init() do
    {:ok, nil}
  end

  def handle_events(events, nil) do
    Logger.error(fn ->
      "For some reason, the primer process #{inspect(self())} received" <>
        " events: #{inspect(events)}"
    end)
    {:noreply, nil}
  end
end
