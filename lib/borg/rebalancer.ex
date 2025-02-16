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
  """
  def alive?(pid \\ __MODULE__) do
    case GenServer.whereis(pid) do
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
  """
  def redistribute(kv_data, %MapSet{} = node_set, this_node, chunk_size) do
    start_time = DateTime.utc_now()
    Logger.info("Beginning redistribution of data on node #{this_node}; #{start_time}")

    kv_data
    |> Stream.chunk_every(chunk_size)
    |> Stream.each(fn chunk ->
      {data_to_copy, keys_to_delete_from_this_node} = get_allocation(chunk, node_set, this_node)
      apply_allocation(data_to_copy)
      acc_keys_for_deletion(keys_to_delete_from_this_node)

      # First append the owners (i.e. the nodes) to each key/value tuple:
      # Enum.map(chunk, fn {key, value} -> {key, value, Borg.whereis(node_set, key)} end)
      # Then, reshape the input so there is one tuple for each owner
      # Enum.flat_map([{:x, "xray", [:a, :b]}, {:y, "yellow", [:a, :c]}], fn {key, val, owners} -> Enum.reduce(owners, [], fn owner, acc -> [ {owner, key, val} | acc] end) end)
      # [{:b, :x, "xray"}, {:a, :x, "xray"}, {:c, :y, "yellow"}, {:a, :y, "yellow"}]
      # then group by owner node
      # Enum.group_by(z, fn {dest, _, _} -> dest end, fn {_, k, v} -> {k, v} end)
      # %{c: [y: "yellow"], a: [x: "xray", y: "yellow"], b: [x: "xray"]}
    end)
    |> Stream.run()

    # only do this AFTER all the copy operations have finished
    delete_keys()

    end_time = DateTime.utc_now()
    elapsed_ms = DateTime.diff(end_time, start_time, :millisecond)

    Logger.info(
      "Completing redistribution on node #{this_node}; #{end_time} (#{elapsed_ms}ms elapsed)"
    )
  end

  @doc """
  Takes the data produced from `get_allocation/3` and applies it by dispatching
  operations to storage.
  """
  # def apply_allocation(data_to_copy, _keys_to_delete_from_this_node) do
  def apply_allocation(data_to_copy) do
    data_to_copy
    |> Enum.map(fn {target_node, data} -> Storage.merge({Storage, target_node}, data) end)
    |> Enum.all?(fn result -> result == :ok end)
    |> case do
      true -> :ok
      false -> Logger.error("Error merging to one or more nodes")
    end
  end

  @doc """
  This intermediary function calculates what is to be done with each member of
  the provided data based on the context of `this_node`. Each key/value pair
  either needs to be copied to one or more of the destination nodes and it may
  need to be deleted from this_node.

  The actions are represented by the following data structure:

      {destination_map, keys_to_delete_from_this_node}

  The destination map uses keys representing other nodes in the cluster; values
  are maps of all the data that should be copied to that node.

  E.g. imagine this node "a" currently contains the following data:

      %{1 => "v1", 2 => "v2", 3 => "v3"}

  Hash ring calculations determine the following:

  - Key `1` should belong on nodes `["a", "b"]`
  - Key `2` should belong on nodes `["a", "c"]`
  - Key `3` should belong on nodes `["b", "c"]`

  The resulting data structure returned from this function would look like the
  following:

      {
        %{
          "b" => %{1 => "v1", 3 => "v3"}
          "c" => %{2 => "v2", 3 => "v3"}
        },
        [3]
      }

  From this we can see that keys 1 and 3 should be copied to node "b", keys 2 and
  3 should be copied to node "c", and key 3 should be deleted from this node (node "a")
  because nodes "b" and "c" are determined to be the sole owners of data at that key.

  TODO: refactor more along the lines of Enum.group_by/3
  """
  def get_allocation(data, %MapSet{} = node_set, this_node) do
    merge_map = create_merge_map(node_set, this_node)

    data
    |> Enum.reduce({merge_map, []}, fn {key, value}, {merges, delete_keys} ->
      owners = Borg.whereis(node_set, key)
      other_owners = owners -- [this_node]

      merges =
        Enum.reduce(other_owners, merges, fn node, acc ->
          merges_this_node = Map.fetch!(acc, node)
          merges_this_node = Map.put(merges_this_node, key, value)
          Map.put(acc, node, merges_this_node)
        end)

      delete_keys =
        if Enum.member?(owners, this_node) do
          delete_keys
        else
          [key | delete_keys]
        end

      {merges, delete_keys}
    end)
  end

  # Creates a map with keys for each node in the collective _except_ this node
  # Values are maps.
  # This is an intermediary data structure that will eventually tell us which
  # data needs to be sent to (i.e. merged with) each node.
  defp create_merge_map(%MapSet{} = node_set, this_node) do
    node_set
    |> MapSet.delete(this_node)
    |> Enum.map(fn node -> {node, %{}} end)
    |> Map.new()
  end
end
