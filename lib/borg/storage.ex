defmodule Borg.Storage do
  @moduledoc """
  Simple Map-like key/value storage in a GenServer

  Storage can be accessed locally

      iex> Borg.Storage.get(Borg.Storage, :foo)

  or by other nodes if they include a more fully qualified process identifier, e.g.

      iex> Borg.Storage.get({Borg.Storage, :b@localhost}, :foo)
  """
  use GenServer

  require Logger

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  def fetch(pid, key) do
    GenServer.call(pid, {:fetch, key})
  end

  def keys(pid) do
    GenServer.call(pid, :keys)
  end

  def put(pid, key, value) do
    GenServer.call(pid, {:put, key, value})
  end

  def rebalance(pid, %HashRing{} = ring) do
    GenServer.cast(pid, {:rebalance, ring})
  end

  # def rebalance_down(pid, %HashRing{} = ring, dead_node) do
  #   GenServer.call(pid, {:rebalance_down, ring, dead_node})
  # end

  # def rebalance_up(pid, %HashRing{} = ring, new_node) do
  #   GenServer.call(pid, {:rebalance_up, ring, new_node})
  # end

  def state(pid) do
    GenServer.call(pid, :state)
  end

  # GenServer callbacks
  def init(_) do
    state = %{}
    {:ok, state}
  end

  def handle_call({:delete, key}, from, state) do
    Logger.debug("Deleting key #{key} from my node #{node()}. Msg from #{inspect(from)}")
    {:reply, :ok, Map.delete(state, key)}
  end

  def handle_call({:fetch, key}, _from, state) do
    {:reply, Map.fetch(state, key), state}
  end

  def handle_call(:keys, _from, state) do
    {:reply, Map.keys(state), state}
  end

  def handle_call({:put, key, value}, _from, state) do
    {:reply, :ok, Map.put(state, key, value)}
  end

  # def handle_call({:rebalance_up, ring, new_node}, _from, state) do
  #   #
  #   this_node = node()
  #   old_ring = HashRing.remove_node(ring, new_node)

  #   new_state =
  #     state
  #     |> Enum.reduce(%{}, fn {key, value}, acc ->
  #       # Where the key used to be stored
  #       old_nodes_set = old_ring |> Borg.whereis(key) |> MapSet.new()
  #       # Where the key now should be stored
  #       new_nodes_set = ring |> Borg.whereis(key) |> MapSet.new()

  #       # if !MapSet.equal?(old_nodes_set, new_nodes_set) do
  #       nodes_needing_data = MapSet.difference(new_nodes_set, old_nodes_set)

  #       Enum.each(nodes_needing_data, fn node ->
  #         :ok = Borg.Storage.put({Borg.Storage, node}, key, value)
  #       end)

  #       # nodes_needing_pruning = MapSet.difference(old_nodes_set, new_nodes_set)
  #       # end

  #       # Does this value belong on this node?
  #       if MapSet.member?(new_nodes_set, this_node) do
  #         Map.put(acc, key, value)
  #       else
  #         acc
  #       end
  #     end)

  #   {:reply, :ok, new_state}
  # end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_cast({:rebalance, _ring}, state) when map_size(state) == 0 do
    Logger.debug("NOOP: rebalancing empty state on node #{node()}")
    {:noreply, %{}}
  end

  def handle_cast({:rebalance, ring}, state) do
    # old_ring = HashRing.add_node(ring, dead_node)
    this_node = node()

    # Logger.debug("Rebalancing node #{this_node}")
    Logger.debug("Rebalancing node #{this_node}; Current state: #{inspect(state)}")
    # # Rebalance: copy data from any key that resided on the dead node
    # keys_this_node = Borg.Storage.keys(Borg.Storage)

    new_state =
      state
      |> Enum.reduce(%{}, fn {key, value}, acc ->
        owners = Borg.whereis(ring, key)
        Logger.debug("Key #{key} belongs on nodes #{inspect(owners)}")

        foo =
          Enum.reduce(owners, acc, fn node, acc2 ->
            if node == this_node do
              Logger.debug("Putting key #{key} into local state on node #{node}")
              Map.put(acc2, key, value)
            else
              Logger.debug("Putting key #{key} into REMOTE state on node #{node}")
              put({__MODULE__, node}, key, value)
              acc2
            end
          end)

        IO.inspect(foo, label: "NEW STATE")
        foo
      end)

    #   result = for {key, value} <- state, g <- 0..5, b <- 0..5 do
    # IO.puts(
    #   "RGB #{r},#{g},#{b}: " <>
    #     IO.ANSI.color(r, g, b) <>
    #     "████████████████████ Sample Text" <>
    #     reset()
    # )
    # end

    {:noreply, new_state}
  end
end
