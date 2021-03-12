defmodule Spigot.Worker do
  @callback init() :: {:ok, state :: term}

  @callback handle_event(event :: term, state :: term) ::
  {:fire_trigger | :continue | :ignore,
    events_to_sink :: [event :: term],
    new_state :: term}

  @callback trigger(events :: [events :: term], state :: term) ::
  {:ok,
    events_to_sink :: [event :: term],
    new_state :: term}

  @optional_callbacks trigger: 2
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
