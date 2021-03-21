defmodule Spigot.KeyedDispatcher do
  @behaviour GenStage.Dispatcher

  require Logger

  def init(_opts) do
    {:ok, %{}}
  end

  def subscribe(opts, from, subscribers) do
    case validate_key(opts, subscribers) do
      :not_provided ->
        Logger.error(fn ->
          "The key option has not been provided. " <>
            "Discarding subscription to #{self()}."
        end)
      {key, :already_registered} ->
        Logger.error(fn ->
          "The key #{key} is already registered. " <>
            "Discarding subscription to #{self()}."
        end)
      key ->
        # Tuples of {from = {pid, reference}, demand}
        {:ok, 0, Map.put(subscribers, key, {from, 0})}
    end
  end

  def ask(demand, from, subscribers) do
    {key, {process, process_demand}} = Enum.find(subscribers, fn {_key, val} ->
      {process, _demand} = val
      process == from
    end)
    {:ok,
      process_demand + demand,
      Map.put(subscribers, key, {process, process_demand + demand})}
  end

  def dispatch(events, _length, subscribers) do
    {to_dispatch, leftovers} = Enum.split_with(events, fn {key, _event_data} ->
      case Map.get(subscribers, key) do
        nil -> false
        {_subscriber, demand} when demand < 1 -> false
        _ -> true
      end
    end)

    events_by_subscriber = Enum.group_by(to_dispatch, fn {key, event_data} ->
      Map.fetch!(subscribers, key)
    end)
    
    subscriber_updates =
      events_by_subscriber
      |> Enum.map(fn({{{pid, ref} = subscriber, demand}, events_to_dispatch}) ->
        {key, _event_data} = hd(events_to_dispatch)
        events_to_dispatch = Enum.map(
          events_to_dispatch,
          fn {_key, event_data} -> event_data end
        )
        Process.send(pid, {:"$gen_consumer", {self(), ref}, events_to_dispatch}, [:noconnect])
        {key, {subscriber, demand - length(events_to_dispatch)}}
      end)
      |> Enum.reduce(%{}, fn {key, subscriber}, acc -> Map.put(acc, key, subscriber) end)

    {:ok, leftovers, Map.merge(subscribers, subscriber_updates)}
  end

  def cancel(from, subscribers) do
    # TODO put this lookup in a private function
    {key, _process} = Enum.find(subscribers, fn {_key, val} ->
      {process, _demand} = val
      process == from
    end)

    {:ok, 0, Map.delete(subscribers, key)}
  end

  def info(msg, state) do
    Logger.error(
      "Unrecognized message #{inspect(msg)} received. Ignoring."
    )
    {:ok, state}
  end

  defp validate_key(opts, subscribers) do
    key = Keyword.get(opts, :key)
    cond do
      key == nil -> nil
      Map.get(subscribers, key) != nil -> {key, :already_registered}
      true -> key
    end
  end
end
