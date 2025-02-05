defmodule Borg.Balancer do
  @moduledoc """
  Maintains state for the current hash ring of the cluster; receives messages about
  any nodes joining or leaving the cluster and balances keys across the nodes.

  See https://www.erlang.org/doc/apps/kernel/net_kernel.html#monitor_nodes/2
  Also https://bigardone.dev/blog/2021/05/22/three-real-world-examples-of-distributed-elixir-pt-1
  """
  use GenServer
  require Logger

  def start_link(_), do: GenServer.start_link(__MODULE__, HashRing.new(node()), name: __MODULE__)

  @doc """
  Gets the current hash ring
  """
  def ring do
    GenServer.call(__MODULE__, :ring)
  end

  @impl GenServer
  def init(ring) do
    :net_kernel.monitor_nodes(true)

    {:ok, ring}
  end

  @impl GenServer
  def handle_info({:nodedown, dead_node}, old_ring) do
    # A node left the cluster
    Logger.info("Node #{dead_node} has left the cluster")
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

  def handle_info({:nodeup, new_node}, old_ring) do
    # A new node joined the cluster
    Logger.info("New Node #{new_node} added to the cluster")

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

    {:noreply, new_ring}
  end

  @impl GenServer
  def handle_call(:ring, _from, ring) do
    {:reply, ring, ring}
  end
end
