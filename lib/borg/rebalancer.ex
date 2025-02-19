defmodule Borg.Rebalancer do
  @moduledoc """
  This module is dedicated to rebalancing data across nodes.  Keys are assigned to
  nodes the math of [Rendezvous Hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing).
  This functionality is implemented as a GenServer so we can tell if a node is
  in the process of being rebalanced (i.e. just check to see if the process is alive).
  The state of the GenServer is the list of keys which are to be deleted from this node.

  To start:

      DynamicSupervisor.start_child(Borg.DynamicSupervisor, {Borg.Rebalancer, []})

  To stop:

    DynamicSupervisor.terminate_child(Borg.DynamicSupervisor, pid)
  """

  use GenServer

  require Logger

  alias Borg.Storage

  @doc """
  Is the rebalancer process running on this node?
  WARNING: this doesn't really work for GenServers running on other nodes.
  """
  def alive? do
    case GenServer.whereis(__MODULE__) do
      nil -> false
      _ -> true
    end
  end

  @doc """
  Accumulate the given keys for later deletion.
  """
  def acc_keys_for_deletion(pid \\ __MODULE__, keys) do
    GenServer.call(pid, {:acc_keys_for_deletion, keys})
  end

  @doc """
  Delete accumulated keys.
  """
  def delete_keys(pid \\ __MODULE__) do
    GenServer.call(pid, :delete_keys)
  end

  # called indirectly by `DynamicSupervisor.start_child/2`
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, MapSet.new(), name: name)
  end

  @impl GenServer
  def init(state) do
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:acc_keys_for_deletion, keys}, _from, state) do
    {:reply, :ok, MapSet.union(state, MapSet.new(keys))}
  end

  def handle_call(:delete_keys, _from, state) do
    result = Storage.drop(Storage, MapSet.to_list(state))
    {:reply, result, MapSet.new()}
  end

  @doc """
  Balances then given key/value data (ideally a stream) using the given hash ring.
  kv_stream = Storage.to_stream(Storage)
  """
  def redistribute(%MapSet{} = node_set, this_node, chunk_size) do
    start_time = DateTime.utc_now()
    Logger.info("Beginning redistribution of data on node #{this_node}; #{start_time}")
    kv_data = Storage.to_stream(Storage)

    kv_data
    |> Stream.chunk_every(chunk_size)
    |> Stream.each(fn chunk ->
      chunk
      |> Enum.flat_map(fn {k, v} ->
        owners = Borg.whereis(node_set, k)

        if Enum.member?(owners, this_node) do
          Enum.flat_map(owners, fn
            target_node when target_node != this_node -> [{target_node, k, v}]
            _target_node -> []
          end)
        else
          # delete this key
          [{this_node, k, v}]
        end
      end)
      |> Enum.group_by(fn {dest_node, _, _} -> dest_node end, fn {_, k, v} -> {k, v} end)
      |> Enum.map(fn
        {target_node, data} when target_node != this_node ->
          Storage.merge({Storage, target_node}, Map.new(data))

        {^this_node, data} ->
          data
          |> Enum.map(fn {k, _} -> k end)
          |> acc_keys_for_deletion()
      end)
    end)
    |> Stream.run()

    # only do this AFTER all the copy operations have finished
    delete_keys()

    # TODO: telemetry
    end_time = DateTime.utc_now()
    elapsed_ms = DateTime.diff(end_time, start_time, :millisecond)

    Logger.info(
      "Completing redistribution on node #{this_node}; #{end_time} (#{elapsed_ms}ms elapsed)"
    )
  end
end
