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
  # @process_group_name :borg_collective

  def start_link(_), do: GenServer.start_link(__MODULE__, HashRing.new(), name: __MODULE__)

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
    :pg.get_members(__MODULE__)
  end

  @doc """
  Gets the hash ring from the given instance
  """
  def ring(pid \\ __MODULE__) do
    GenServer.call(pid, :ring)
  end

  @impl GenServer
  def init(state) do
    # Subscribe to nodeup/nodedown
    # :ok = :net_kernel.monitor_nodes(true)
    # Subscribe to join/leave events
    :pg.monitor(__MODULE__)
    :ok = :pg.join(__MODULE__, Process.whereis(__MODULE__))
    {:ok, state}
  end

  @doc """
  The following info messages are handled:

  - `{_ref, :join, _group, new_pids}` received via `:pg.monitor/1` subscription
  - `{_ref, :leave, _group, departing_pids}` received via `:pg.monitor/1` subscription

  Note that when a node first comes up, there are multiple `:join` messages received:
  one for each node in the cluster.  This means we have to avoid doing duplicate work.
  """
  @impl GenServer
  def handle_info({_ref, :join, _group, [new_pid]}, ring) do
    Logger.debug("#{inspect(new_pid)} has joined the group")
    # When a new node comes online, it receives a message for each of the other
    # nodes in the cluster; e.g. if node C is turned on, it will get notified that
    # node A has joined, then another message that node B has joined.
    # `:pg.get_members/1` _eventually_ will return all the nodes in the cluster, but
    # the first messages are incomplete.
    # We need a way to "debounce" messages: we will wait a tick and compare the
    # set of pids captured at the time of sending vs. the time of receiving
    # new_node = node(new_pid)
    pid_set = MapSet.new(:pg.get_members(__MODULE__))
    Process.send_after(self(), {:update_topology, pid_set}, 100)

    # pids = :pg.get_members(__MODULE__)
    # # IO.inspect(pids, label: "PG MEMBERSHIP after joining")
    # Logger.debug("PG MEMBERSHIP after joining: #{inspect(pids)}")

    # # Don't try to rebalance the NEW node -- the other nodes will take care of it
    # if new_node != node() do
    #   # Logger.debug("New node #{new_node} is not this node (#{node()})... rebalancing...")
    #   # :ok = Rebalancer.up(new_node, old_ring)
    # end

    # nodes = Enum.map(pids, fn pid -> node(pid) end)

    # ring = HashRing.add_nodes(HashRing.new(), nodes)

    # Logger.debug(":pg join pids: #{inspect(pids)}; updated ring: #{inspect(ring)}")

    {:noreply, ring}
  end

  def handle_info({_ref, :leave, _group, _pids}, _ring) do
    # Logger.debug("#{inspect(departing_pid)} has left the group")
    # departing_nodes = Enum.map(departing_pids, fn pid -> node(pid) end)
    # # departing_node = node(departing_pid)
    # ring = HashRing.add_nodes(HashRing.new(), HashRing.nodes(old_ring) -- departing_nodes)

    member_pids = :pg.get_members(__MODULE__)
    ring = HashRing.add_nodes(HashRing.new(), Enum.map(member_pids, fn pid -> node(pid) end))

    Logger.debug("PG MEMBERSHIP after leaving: #{inspect(ring)}")

    # Logger.debug(":pg leave pids: #{inspect(departing_pids)}; updated ring: #{inspect(ring)}")
    kv_stream = Borg.Storage.to_stream()
    Rebalancer.redistribute(kv_stream, ring, node(), 100)

    {:noreply, ring}
  end

  def handle_info({:update_topology, pid_set}, ring) do
    member_pids = :pg.get_members(__MODULE__)

    if MapSet.equal?(MapSet.new(member_pids), pid_set) do
      Logger.debug("Updating topology...")

      # new_node = node(new_pid)
      # # Don't try to rebalance the NEW node -- the other nodes will take care of it
      # if new_node != node() do
      #   # Logger.debug("New node #{new_node} is not this node (#{node()})... rebalancing...")
      #   # :ok = Rebalancer.up(new_node, old_ring)
      # end

      ring = HashRing.add_nodes(HashRing.new(), Enum.map(member_pids, fn pid -> node(pid) end))

      # Rebalancer.calc_redistribution(ring, node())
      kv_stream = Borg.Storage.to_stream()
      Rebalancer.redistribute(kv_stream, ring, node(), 100)

      {:noreply, ring}
    else
      Logger.debug("Ignoring outdated :join message")
      {:noreply, ring}
    end

    # A node left the cluster
    # Logger.info("Node #{dead_node} has left the cluster")
    # Do cleanup in
    # {:noreply, ring}
  end

  # def handle_info({:nodedown, dead_node}, ring) do
  #   # A node left the cluster
  #   Logger.info("Node #{dead_node} has left the cluster")
  #   # Do cleanup in
  #   {:noreply, ring}
  # end

  # def handle_info({:nodeup, new_node}, ring) do
  #   # A new node joined the cluster
  #   Logger.info("New Node #{new_node} added to the cluster. This node may or may not be Borg")

  #   {:noreply, ring}
  # end

  @impl GenServer
  def handle_call(:ring, _from, ring) do
    {:reply, ring, ring}
  end
end
