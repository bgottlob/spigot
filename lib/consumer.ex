defmodule Spigot.Consumer do
  use GenStage

  def start_link(mod) do
    GenStage.start_link(__MODULE__, mod)
  end

  def init(mod) do
    {:ok, state} = mod.init()
    {:consumer, {mod, state, []}}
  end

  def handle_events(events, _from, {mod, state, window_buffer}) do
    {new_state, new_window_buffer} = handle_events(events, mod, state, window_buffer)
    {:noreply, [], {mod, new_state, new_window_buffer}}
  end

  defp handle_events([], _mod, state, window_buffer), do: {state, window_buffer}
  defp handle_events([event | rest], mod, state, window_buffer) do
    {new_state, window_buffer} = case mod.handle_event(event, state) do
      {:fire_trigger, new_state} ->
        # Perform trigger for events in window
        {:ok, new_state} = mod.trigger(Enum.reverse([event | window_buffer]), new_state)
        # Handle next event and start a new window
        {new_state, []}
      {:continue, new_state} ->
        # Buffer this event
        {new_state, [event | window_buffer]}
      {:ignore, new_state} ->
        # Do not buffer this event
        {new_state, window_buffer}
    end

    handle_events(rest, mod, new_state, window_buffer)
  end
end
