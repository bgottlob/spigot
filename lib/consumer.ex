defmodule Spigot.Consumer do
  use GenStage

  def start_link(mod) do
    GenStage.start_link(__MODULE__, mod)
  end

  def init(mod) do
    {:ok, state} = mod.init()
    {:producer_consumer, {mod, state, []}}
  end

  def handle_events(events, _from, {mod, state, window_buffer}) do
    {new_state, new_window_buffer, new_to_dispatch} =
      handle_events(events, mod, state, window_buffer, [])
    {:noreply,
      new_to_dispatch,
      {mod, new_state, new_window_buffer}}
  end

  defp handle_events([], _mod, state, window_buffer, dispatch_buffer) do
    {state, window_buffer, dispatch_buffer |> Enum.reverse |> List.flatten}
  end

  defp handle_events([event | rest], mod, state, window_buffer, dispatch_buffer) do
    {new_state, to_sink, window_buffer} = case mod.handle_event(event, state) do
      {:fire_trigger, to_sink_handler, new_state} ->
        # Perform trigger for events in window
        {:ok, to_sink_trigger, new_state} = mod.trigger(Enum.reverse([event | window_buffer]), new_state)
        # Handle next event and start a new window
        {new_state, [to_sink_handler, to_sink_trigger], []}
      {:continue, to_sink_handler, new_state} ->
        # Buffer this event
        {new_state, to_sink_handler, [event | window_buffer]}
      {:ignore, to_sink_handler, new_state} ->
        # Do not buffer this event
        {new_state, to_sink_handler, window_buffer}
    end

    handle_events(
      rest,
      mod,
      new_state,
      window_buffer,
      [to_sink | dispatch_buffer]
    )
  end
end
