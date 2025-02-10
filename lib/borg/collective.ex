defmodule Borg.Collective do
  @moduledoc """
  Maintains state about the current cluster; receives messages about
  any nodes joining or leaving the cluster and balances keys across the nodes.
  """
  use GenServer
  require Logger

  alias Borg.Rebalancer
  alias Borg.Storage

  def start_link(_), do: GenServer.start_link(__MODULE__, MapSet.new(), name: __MODULE__)

  @doc """
  Gets the set of nodes in the Borg collective
  """
  def members(pid \\ __MODULE__) do
    GenServer.call(pid, :members)
  end

  @impl GenServer
  def init(node_set) do
    # Subscribe to join/leave events
    :pg.monitor(__MODULE__)
    :ok = :pg.join(__MODULE__, Process.whereis(__MODULE__))
    {:ok, MapSet.put(node_set, node())}
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
  def handle_info({_ref, :join, _group, [_new_pid]}, node_set) do
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

    {:noreply, node_set}
  end

  def handle_info({_ref, :leave, _group, _pids}, node_set) do
    member_pids = :pg.get_members(__MODULE__)
    node_set = Enum.reduce(member_pids, node_set, fn pid, acc -> MapSet.put(acc, node(pid)) end)

    Logger.debug("PG MEMBERSHIP after leaving: #{inspect(node_set)}")

    # TODO: balance down
    # kv_stream = Storage.to_stream(Storage)
    # Rebalancer.redistribute(kv_stream, node_set, node(), 100)

    {:noreply, node_set}
  end

  def handle_info({:update_topology, pid_set}, old_node_set) do
    member_pids = :pg.get_members(__MODULE__)

    if MapSet.equal?(MapSet.new(member_pids), pid_set) do
      new_ring_set =
        Enum.reduce(member_pids, MapSet.new(), fn pid, acc -> MapSet.put(acc, node(pid)) end)

      added_nodes = MapSet.difference(new_ring_set, old_node_set)

      Logger.debug(
        "Updating topology to new ring: #{inspect(new_ring_set)}; added node(s): #{inspect(added_nodes)}"
      )

      # TODO: spawn new process
      # kv_stream = Storage.to_stream(Storage)
      # Rebalancer.redistribute(kv_stream, new_ring_set, node(), 100)

      {:noreply, new_ring_set}
    else
      Logger.debug("Ignoring outdated :join message")
      {:noreply, old_node_set}
    end
  end

  @impl GenServer
  def handle_call(:members, _from, node_set) do
    {:reply, node_set, node_set}
  end
end
