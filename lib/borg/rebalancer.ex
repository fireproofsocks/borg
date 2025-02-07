defmodule Borg.Rebalancer do
  @moduledoc """
  Rebalances keys across nodes.
  """

  require Logger

  # @doc """
  # To be called when a member node has left the ring; keys/values need to be shuffled/copied
  # to ensure redundancy.
  # """
  # def down(dead_node, %HashRing{} = old_ring) do
  #   # A node left the cluster
  #   Logger.info("Rebalancing down; removing node #{dead_node} from ring #{inspect(old_ring)}")
  #   # formerly:
  #   # :foo -> [:b, :c]
  #   # after node :c leaves the cluster:
  #   # :foo -> [:a, :b]
  #   # Borg.Storage.rebalance_down()
  #   new_ring = HashRing.remove_node(old_ring, dead_node)

  #   # :ok = Borg.Storage.rebalance(Borg.Storage, new_ring)
  #   # Rebalance: copy data from any key that resided on the dead node
  #   keys_this_node = Borg.Storage.keys(Borg.Storage)

  #   Enum.each(keys_this_node, fn key ->
  #     old_nodes_set = old_ring |> Borg.whereis(key, 2) |> MapSet.new()

  #     if MapSet.member?(old_nodes_set, dead_node) do
  #       new_nodes_set = new_ring |> Borg.whereis(key) |> MapSet.new()

  #       nodes_needing_data = MapSet.difference(new_nodes_set, old_nodes_set)
  #       {:ok, value} = Borg.Storage.fetch(Borg.Storage, key)

  #       Enum.each(nodes_needing_data, fn node ->
  #         :ok = Borg.Storage.put({Borg.Storage, node}, key, value)
  #       end)
  #     end
  #   end)

  #   {:noreply, new_ring}
  # end

  # @doc """
  # To be called when a new node joins the collective; keys/values will be re-distributed
  # across the available nodes
  # """
  # def up(new_node, %HashRing{} = old_ring) do
  #   # A new node joined the cluster
  #   Logger.info("Rebalancing up; new node added #{new_node} to ring #{inspect(old_ring)}")

  #   new_ring = HashRing.add_node(old_ring, new_node)

  #   # formerly:
  #   # :foo -> [:a, :b]
  #   # after node :c comes up:
  #   # :foo -> [:c, :b]
  #   keys_this_node = Borg.Storage.keys(Borg.Storage)
  #   Logger.debug("Keys on node #{node()}: #{inspect(keys_this_node)}")

  #   Enum.each(keys_this_node, fn key ->
  #     old_nodes_set = old_ring |> Borg.whereis(key) |> MapSet.new()
  #     new_nodes_set = new_ring |> Borg.whereis(key) |> MapSet.new()

  #     if !MapSet.equal?(old_nodes_set, new_nodes_set) do
  #       {:ok, value} = Borg.Storage.fetch(Borg.Storage, key)

  #       nodes_needing_data = MapSet.difference(new_nodes_set, old_nodes_set)

  #       Enum.each(nodes_needing_data, fn node ->
  #         :ok = Borg.Storage.put({Borg.Storage, node}, key, value)
  #       end)

  #       nodes_needing_pruning = MapSet.difference(old_nodes_set, new_nodes_set)

  #       Enum.each(nodes_needing_pruning, fn node ->
  #         :ok = Borg.Storage.delete({Borg.Storage, node}, key)
  #       end)
  #     end
  #   end)
  # end

  @doc """
  Balances keys using the given hash ring and figures out what to do with them
  given the context of this_node.  Each key either needs to be copied to one or
  more destination nodes or it needs to be deleted from this_node.  The input is
  mapped to a list of tuples like the following

      [
        {:copy, key, destination_nodes},
        {:delete, key}
      ]

  E.g. imagine this node "a" currently contains keys [1, 2, 3].
  `1` should belong on `["a", "b"]`, so its entry becomes `{:copy, key, ["b"]}`
  `2` should belong on `["a", "c"]`, so its entry becomes `{:copy, key, ["c"]}`
  `3` should belong on `["b", "c"]`, so its entry becomes `{:delete, key}`

      {
        [
          {:copy, node1, keys},
          {:copy, node2, keys},
          ...
        ],
        keys_to_delete_from_this_node
      }

      {
        %{
          node_1 => %{k1: val1}
          node_2 => %{k2: val2}
        },
        delete_keys
      }
  """
  def redistribute(kv_stream, %HashRing{} = ring, this_node, chunk_size) do
    # write lock
    kv_stream
    |> Stream.chunk_every(chunk_size)
    |> Stream.each(fn chunk ->
      {data_to_copy, keys_to_delete_from_this_node} = allocate_chunk(chunk, ring, this_node)

      Enum.each(data_to_copy, fn {target_node, data} ->
        Logger.debug("Sending node #{target_node} data #{inspect(data)}")
        :ok = Borg.Storage.merge({Borg.Storage, target_node}, data)
      end)

      Borg.Storage.drop(keys_to_delete_from_this_node)
    end)
    |> Stream.run()

    # release write lock

    # keys
    # |> Enum.map(fn key -> {key, Borg.whereis(ring, key)} end)

    # node_map = other_nodes |> Enum.map(fn node -> {node, []} end) |> Map.new()

    # Enum.reduce(keys, {node_map, []}, fn key, acc ->
    #   owners = Borg.whereis(ring, key)
    # end)
  end

  @doc """
  Given data on this_node, this function figures out which data needs to be copied
  to which node and which keys should be deleted from this node.
  The input data should be an enumerable (e.g. a map or a stream). The value for
  this_node should be a node represented in the given hash ring.
  """
  def allocate_chunk(data, %HashRing{} = ring, this_node) do
    merge_map = merge_map(ring, this_node)

    data
    |> Enum.reduce({merge_map, []}, fn {key, value}, {merges, delete_keys} ->
      owners = Borg.whereis(ring, key)
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
