defmodule Borg.Collective do
  @moduledoc """
  Maintains state for the current hash ring of the cluster; receives messages about
  any nodes joining or leaving the cluster and balances keys across the nodes.

  See https://www.erlang.org/doc/apps/kernel/net_kernel.html#monitor_nodes/2
  Also https://bigardone.dev/blog/2021/05/22/three-real-world-examples-of-distributed-elixir-pt-1
  """
  use GenServer
  require Logger

  alias Borg.Rebalancer

  # Atom name used with calls to :pg
  @process_group_name :borg_collective

  def start_link(_), do: GenServer.start_link(__MODULE__, :undefined, name: __MODULE__)

  @doc """
  Adds the given node to the collective
  """
  def add_node(pid, node) do
    # :pg.join(:my_group, Process.whereis(Borg.Foo))
    GenServer.call(pid, {:add_node, node})
  end

  @doc """
  Gets a list of pids in the process group
  """
  def pids do
    :pg.get_members(@process_group_name)
  end

  @doc """
  Gets the hash ring from the given instance
  """
  def ring(pid \\ __MODULE__) do
    GenServer.call(pid, :ring)
  end

  @impl GenServer
  def init(state) do
    :ok = :net_kernel.monitor_nodes(true)
    # {:ok, _} = :pg.start_link()
    :ok = :pg.join(@process_group_name, Process.whereis(__MODULE__))
    # We have to wait a tick for the app to boot etc (?) before :pg will see
    # see the remote pids that may be in the group.
    Process.send_after(self(), :update_topology, 100)

    # {:ok, ring, {:continue, :notify_collective}}
    {:ok, state}
  end

  # @impl GenServer
  # def handle_continue(:notify_collective, state) do
  #   # Do stuff
  #   pids = pids()
  #   IO.puts("Notifying the following pids in the process group: #{inspect(pids)}")

  #   # Notify the other Borg of the new node
  #   x =
  #     Enum.each(pids, fn pid ->
  #       Logger.debug("Telling pid #{inspect(pid)} about node #{node()}")
  #       add_node(pid, node())
  #     end)

  #   # Then join the process group
  #   result = :pg.join(@process_group_name, Process.whereis(__MODULE__))
  #   dbg(result)
  #   IO.puts("PIds after i joined the group: #{inspect(pids)}")
  #   dbg(x)
  #   {:noreply, state}
  # end

  @doc """
  The `{:nodedown, node}` and `{:nodeup, node}` messages are received because this
  process subscribed to them via `:net_kernel.monitor_nodes(true)`.

  `:pg.get_members/1` does not seem to include the pids registered by other nodes
  until *after* the application has started, so we have to update the topology here
  a little bit afterwards.
  """
  @impl GenServer
  def handle_info(:update_topology, _ring) do
    # pids = :pg.get_members(@process_group_name)
    # IO.puts("Pids after i joined the group: #{inspect(pids)}")

    pids = :pg.get_members(@process_group_name)
    IO.inspect(pids, label: "ALL MEMBERS OF PROCESS GROUP")
    pids_other_nodes = Enum.reject(pids, fn pid -> pid == self() end)

    # inherit the ring from another Borg (if available)
    ring =
      case pids_other_nodes do
        [] -> HashRing.new()
        [pid_other_node | _] -> GenServer.call(pid_other_node, :ring)
      end

    # Notify OTHER nodes that this node is Borg
    IO.puts(
      "Notifying the following OTHER pids in the process group: #{inspect(pids_other_nodes)}"
    )

    Enum.each(pids_other_nodes, fn pid ->
      Logger.debug("Telling pid #{inspect(pid)} about node #{node()}")
      GenServer.call(pid, {:add_node, node()})
    end)

    # Rebalancer.up(node(), ring)
    # # Then join the process group
    # me = Process.whereis(__MODULE__)
    # dbg(me)
    # result = :pg.join(@process_group_name, Process.whereis(__MODULE__))
    # dbg(result)
    # pids = :pg.get_members(@process_group_name)
    # IO.puts("Pids after i joined the group: #{inspect(pids)}")

    {:noreply, HashRing.add_node(ring, node())}
  end

  def handle_info({:nodedown, dead_node}, old_ring) do
    # A node left the cluster
    Logger.info("Node #{dead_node} has left the cluster")
    # Was it a Borg?
    if Enum.member?(HashRing.nodes(old_ring), dead_node) do
      Rebalancer.down(dead_node, old_ring)
    end

    # formerly:
    # :foo -> [:b, :c]
    # after node :c leaves the cluster:
    # :foo -> [:a, :b]
    # Borg.Storage.rebalance_down()
    # new_ring = HashRing.remove_node(old_ring, dead_node)

    # # :ok = Borg.Storage.rebalance(Borg.Storage, new_ring)
    # # Rebalance: copy data from any key that resided on the dead node
    # keys_this_node = Borg.Storage.keys(Borg.Storage)

    # Enum.each(keys_this_node, fn key ->
    #   old_nodes_set = old_ring |> Borg.whereis(key, 2) |> MapSet.new()

    #   if MapSet.member?(old_nodes_set, dead_node) do
    #     new_nodes_set = new_ring |> Borg.whereis(key) |> MapSet.new()

    #     nodes_needing_data = MapSet.difference(new_nodes_set, old_nodes_set)
    #     {:ok, value} = Borg.Storage.fetch(Borg.Storage, key)

    #     Enum.each(nodes_needing_data, fn node ->
    #       :ok = Borg.Storage.put({Borg.Storage, node}, key, value)
    #     end)
    #   end
    # end)

    {:noreply, HashRing.remove_node(old_ring, dead_node)}
  end

  def handle_info({:nodeup, new_node}, ring) do
    # A new node joined the cluster
    Logger.info("New Node #{new_node} added to the cluster. This node may or may not be Borg")
    # We actually don't care if a new node joins the cluster.  If it is a Borg, it
    # will join the collective as part of its bootstrapping.
    # new_ring = HashRing.add_node(old_ring, new_node)

    # # formerly:
    # # :foo -> [:a, :b]
    # # after node :c comes up:
    # # :foo -> [:c, :b]
    # keys_this_node = Borg.Storage.keys(Borg.Storage)

    # Enum.each(keys_this_node, fn key ->
    #   old_nodes_set = old_ring |> Borg.whereis(key) |> MapSet.new()
    #   new_nodes_set = new_ring |> Borg.whereis(key) |> MapSet.new()

    #   if !MapSet.equal?(old_nodes_set, new_nodes_set) do
    #     {:ok, value} = Borg.Storage.fetch(Borg.Storage, key)

    #     nodes_needing_data = MapSet.difference(new_nodes_set, old_nodes_set)

    #     Enum.each(nodes_needing_data, fn node ->
    #       :ok = Borg.Storage.put({Borg.Storage, node}, key, value)
    #     end)

    #     nodes_needing_pruning = MapSet.difference(old_nodes_set, new_nodes_set)

    #     Enum.each(nodes_needing_pruning, fn node ->
    #       :ok = Borg.Storage.delete({Borg.Storage, node}, key)
    #     end)
    #   end
    # end)

    {:noreply, ring}
  end

  @impl GenServer
  def handle_call({:add_node, node}, _from, ring) do
    Rebalancer.up(node, ring)
    {:reply, :ok, HashRing.add_node(ring, node)}
  end

  @impl GenServer
  def handle_call(:ring, _from, ring) do
    {:reply, ring, ring}
  end
end
