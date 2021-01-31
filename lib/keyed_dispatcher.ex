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
    # TODO this can be done in one pass of the events list
    leftovers = Enum.filter(events, fn {key, _event_data} ->
      case Map.get(subscribers, key) do
        nil -> true
        {_process, demand} when demand < 1 -> true
        _ -> false
      end
    end)

    Enum.filter(events, fn {key, _event_data} -> Map.get(subscribers, key) != nil end)
    |> Enum.reduce(subscribers, fn({key, event_data}, acc) ->
      {{pid, ref}, demand} = Map.get(acc, key)
      Process.send(pid, {:"$gen_consumer", {self(), ref}, [event_data]}, [:noconnect])
      Map.put(acc, key, {{pid, ref}, demand - 1})
    end)

    {:ok, leftovers, subscribers}
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
      "Unrecognized message #{IO.inspect(msg)} received. Ignoring."
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
