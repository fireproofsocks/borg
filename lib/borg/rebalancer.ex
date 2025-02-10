defmodule Borg.Rebalancer do
  @moduledoc """
  This module is dedicated to rebalancing data across nodes.  Keys are assigned to
  nodes using the `libring` package and its `HashRing` module, which relies on
  the math of [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing).
  """

  require Logger

  alias Borg.Storage

  @doc """
  Balances then given key/value data (ideally a stream) using the given hash ring.
  """
  def redistribute(kv_data, %HashRing{} = ring, this_node, chunk_size) do
    Logger.info("Beginning redistribution of data on node #{this_node}; #{DateTime.utc_now()}")
    # TODO: write lock
    kv_data
    |> Stream.chunk_every(chunk_size)
    |> Stream.each(fn chunk ->
      {data_to_copy, keys_to_delete_from_this_node} = get_allocation(chunk, ring, this_node)
      apply_allocation(data_to_copy, keys_to_delete_from_this_node)
    end)
    |> Stream.run()

    # TODO: release write lock
    Logger.info("Completing redistribution on node #{this_node}; #{DateTime.utc_now()}")
  end

  @doc """
  Takes the data produced from `get_allocation/3` and applies it by dispatching
  operations to storage.
  """
  def apply_allocation(data_to_copy, keys_to_delete_from_this_node) do
    Enum.each(data_to_copy, fn {target_node, data} ->
      Logger.debug("Copying data from #{node()} to #{target_node}: #{inspect(data)}")
      :ok = Storage.merge({Storage, target_node}, data)
    end)

    Storage.drop(Storage, keys_to_delete_from_this_node)
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
  """
  def get_allocation(data, %HashRing{} = ring, this_node) do
    merge_map = merge_map(ring, this_node)

    data
    |> Enum.reduce({merge_map, []}, fn {key, value}, {merges, delete_keys} ->
      {:ok, owners} = Borg.whereis(ring, key)
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

  # Creates a map with keys for each node in the HashRing _except_ this node
  # Values are maps.
  # This is an intermediary data structure that will eventually tell us which
  # data needs to be sent to (i.e. merged with) each node.
  defp merge_map(%HashRing{} = ring, this_node) do
    ring
    |> HashRing.remove_node(this_node)
    |> HashRing.nodes()
    |> Enum.map(fn node -> {node, %{}} end)
    |> Map.new()
  end
end
