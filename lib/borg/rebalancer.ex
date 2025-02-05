defmodule Borg.Rebalancer do
  @moduledoc """
  Rebalances keys across nodes.
  """

  require Logger

  @doc """
  To be called when a member node has left the ring; keys/values need to be shuffled/copied
  to ensure redundancy.
  """
  def down(dead_node, old_ring) do
    # A node left the cluster
    Logger.info("Rebalancing down; node #{dead_node} has left the cluster")
    # formerly:
    # :foo -> [:b, :c]
    # after node :c leaves the cluster:
    # :foo -> [:a, :b]
    # Borg.Storage.rebalance_down()
    new_ring = HashRing.remove_node(old_ring, dead_node)

    # :ok = Borg.Storage.rebalance(Borg.Storage, new_ring)
    # Rebalance: copy data from any key that resided on the dead node
    keys_this_node = Borg.Storage.keys(Borg.Storage)

    Enum.each(keys_this_node, fn key ->
      old_nodes_set = old_ring |> Borg.whereis(key, 2) |> MapSet.new()

      if MapSet.member?(old_nodes_set, dead_node) do
        new_nodes_set = new_ring |> Borg.whereis(key) |> MapSet.new()

        nodes_needing_data = MapSet.difference(new_nodes_set, old_nodes_set)
        {:ok, value} = Borg.Storage.fetch(Borg.Storage, key)

        Enum.each(nodes_needing_data, fn node ->
          :ok = Borg.Storage.put({Borg.Storage, node}, key, value)
        end)
      end
    end)

    {:noreply, new_ring}
  end

  @doc """
  To be called when a new node joins the collective; keys/values will be re-distributed
  across the available nodes
  """
  def up(new_node, old_ring) do
    # A new node joined the cluster
    Logger.info("Rebalancing up; new node added #{new_node}")

    new_ring = HashRing.add_node(old_ring, new_node)

    # formerly:
    # :foo -> [:a, :b]
    # after node :c comes up:
    # :foo -> [:c, :b]
    keys_this_node = Borg.Storage.keys(Borg.Storage)

    Enum.each(keys_this_node, fn key ->
      old_nodes_set = old_ring |> Borg.whereis(key) |> MapSet.new()
      new_nodes_set = new_ring |> Borg.whereis(key) |> MapSet.new()

      if !MapSet.equal?(old_nodes_set, new_nodes_set) do
        {:ok, value} = Borg.Storage.fetch(Borg.Storage, key)

        nodes_needing_data = MapSet.difference(new_nodes_set, old_nodes_set)

        Enum.each(nodes_needing_data, fn node ->
          :ok = Borg.Storage.put({Borg.Storage, node}, key, value)
        end)

        nodes_needing_pruning = MapSet.difference(old_nodes_set, new_nodes_set)

        Enum.each(nodes_needing_pruning, fn node ->
          :ok = Borg.Storage.delete({Borg.Storage, node}, key)
        end)
      end
    end)
  end
end
