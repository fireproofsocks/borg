defmodule Borg.Collective do
  @moduledoc """
  Maintains state about the current cluster; receives messages about
  any nodes joining or leaving the cluster and balances keys across the nodes.
  """
  use GenServer
  require Logger

  alias Borg.Rebalancer
  alias Borg.Storage

  def start_link(_), do: GenServer.start_link(__MODULE__, HashRing.new(), name: __MODULE__)

  @doc """
  Gets the hash ring from the given instance
  """
  def ring(pid \\ __MODULE__) do
    GenServer.call(pid, :ring)
  end

  @impl GenServer
  def init(ring) do
    # Subscribe to join/leave events
    :pg.monitor(__MODULE__)
    :ok = :pg.join(__MODULE__, Process.whereis(__MODULE__))
    {:ok, HashRing.add_node(ring, node())}
  end

  @doc """
  The following info messages are handled:

  - `{_ref, :join, _group, new_pids}` received via `:pg.monitor/1` subscription
  - `{_ref, :leave, _group, departing_pids}` received via `:pg.monitor/1` subscription
  - `{:update_topology, pid_set}` internal message to handle new processes joining

  Note that when a node first comes up, there are multiple `:join` messages received:
  one for each node in the cluster.  This means we have to avoid doing duplicate work.
  """
  @impl GenServer
  def handle_info({_ref, :join, _group, [_new_pid]}, ring) do
    # When a new node comes online, it receives a message for each of the other
    # nodes in the cluster; e.g. if node C is turned on, it will get notified that
    # node A has joined, then another message that node B has joined.
    # `:pg.get_members/1` _eventually_ will return all the nodes in the cluster, but
    # the first messages are incomplete (!!!).
    # We need a way to "debounce" messages: we will wait a tick and compare the
    # set of pids captured at the time of sending vs. the time of receiving
    # new_node = node(new_pid)
    pid_set = MapSet.new(:pg.get_members(__MODULE__))
    Process.send_after(self(), {:update_topology, pid_set}, 100)

    {:noreply, ring}
  end

  def handle_info({_ref, :leave, _group, _pids}, _ring) do
    member_pids = :pg.get_members(__MODULE__)
    ring = HashRing.add_nodes(HashRing.new(), Enum.map(member_pids, fn pid -> node(pid) end))

    Logger.debug("PG MEMBERSHIP after leaving: #{inspect(ring)}")

    kv_stream = Storage.to_stream(Storage)
    Rebalancer.redistribute(kv_stream, ring, node(), 100)

    {:noreply, ring}
  end

  def handle_info({:update_topology, pid_set}, old_ring) do
    member_pids = :pg.get_members(__MODULE__)

    old_ring_set = old_ring |> HashRing.nodes() |> MapSet.new()

    if MapSet.equal?(MapSet.new(member_pids), pid_set) do
      ring = HashRing.add_nodes(HashRing.new(), Enum.map(member_pids, fn pid -> node(pid) end))

      new_ring_set = ring |> HashRing.nodes() |> MapSet.new()
      added_nodes = MapSet.difference(new_ring_set, old_ring_set)

      Logger.debug(
        "Updating topology to new ring: #{inspect(ring)}; added node(s): #{inspect(added_nodes)}"
      )

      kv_stream = Storage.to_stream(Storage)
      Rebalancer.redistribute(kv_stream, ring, node(), 100)

      {:noreply, ring}
    else
      Logger.debug("Ignoring outdated :join message")
      {:noreply, old_ring}
    end
  end

  @impl GenServer
  def handle_call(:ring, _from, ring) do
    {:reply, ring, ring}
  end
end
