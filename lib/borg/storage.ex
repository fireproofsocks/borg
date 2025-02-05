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

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_cast({:rebalance, _ring}, state) when map_size(state) == 0 do
    Logger.debug("NOOP: rebalancing empty state on node #{node()}")
    {:noreply, %{}}
  end
end
