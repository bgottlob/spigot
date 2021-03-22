defmodule Spigot do
  @moduledoc """
  Documentation for `Spigot`.
  """

  defstruct [:source, :partitioner, :spigot_producer_consumer, :worker_module, :sink]

  # TODO make this typespec more specific
  @type t :: %__MODULE__{
    source: term,
    partitioner: term,
    spigot_producer_consumer: term,
    worker_module: term,
    sink: term
  }

  @spec from_stages([pid]) :: t
  def from_stages([producer]) do
    %__MODULE__{source: producer}
  end

  @spec partition(t, ((event :: term) -> term)) :: t
  def partition(spigot, partitioner) do
    %{spigot | partitioner: partitioner}
  end

  @spec worker_module(t, module) :: t
  def worker_module(spigot, worker_module) do
    %{spigot | worker_module: worker_module}
  end

  @spec into_stages(t, [pid]) :: :ok
  def into_stages(spigot, [consumer]) do
    {:ok, pc} = Spigot.ProducerConsumer.start_link(
      spigot.worker_module,
      spigot.partitioner,
      consumer
    )
    GenStage.sync_subscribe(pc, to: spigot.source)
    {:ok, pc}
  end
end
