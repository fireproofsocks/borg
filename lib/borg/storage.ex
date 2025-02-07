defmodule Borg.Storage do
  @moduledoc """
  This module sets up a `GenServer` in front of an ETS table (using `Pockets` for
  an easier interface).  We aren't using a `GenServer` to maintain state, only
  to handle messages sent from other nodes. Conceptually, the storage is basically
  a `Map` (a key/value store), but because it uses ETS we can stream output and
  keys, so we are less likely to blow up our memory when we have to rebalance data
  across nodes.

  Storage can be accessed locally

      iex> Borg.Storage.get(Borg.Storage, :some_key)

  or by other nodes if they include a more fully qualified process identifier, e.g.

      iex> Borg.Storage.get({Borg.Storage, :b@localhost}, :some_key)

  Some functions are accessible only locally -- their pid name is hardcoded.
  Messages are still handled by the GenServer so that incoming operations can be
  serialized and run in the order they were received.
  """
  use GenServer

  require Logger

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  @doc """
  Local access only: bulk delete
  """
  def drop(keys) when is_list(keys) do
    GenServer.call(__MODULE__, {:drop, keys})
  end

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  def info(pid) do
    GenServer.call(pid, :info)
  end

  @doc """
  Local access only
  """
  def keys_stream do
    GenServer.call(__MODULE__, :keys_stream)
  end

  def merge(pid, %{} = data) do
    GenServer.call(pid, {:merge, data})
  end

  def put(pid, key, value) do
    GenServer.call(pid, {:put, key, value})
  end

  @doc """
  Local access only
  """
  def to_stream do
    GenServer.call(__MODULE__, :to_stream)
  end

  @doc """
  Dumps the contents of the table for inspection.
  This should only be used for local testing. Things could get ugly if you
  try to dump out a large table!
  """
  def to_map(pid) do
    GenServer.call(pid, :to_map)
  end

  @impl true
  def init(_), do: Pockets.new(__MODULE__)

  @impl true
  def handle_call({:delete, key}, _from, state) do
    Logger.debug("Deleting key #{key} from node #{node()}.")
    result = Pockets.delete(__MODULE__, key)
    {:reply, result, state}
  end

  def handle_call({:drop, keys}, _from, state) do
    Logger.debug("Droping #{length(keys)} keys from node #{node()}.")
    key_set = MapSet.new(keys)
    # Warning: this updates the table in place
    result = Pockets.reject(__MODULE__, fn {k, _v} -> MapSet.member?(key_set, k) end)
    {:reply, result, state}
  end

  def handle_call({:get, key}, _from, state) do
    {:reply, Pockets.get(__MODULE__, key), state}
  end

  def handle_call(:info, _from, state) do
    {:reply, Pockets.info(__MODULE__), state}
  end

  def handle_call(:keys_stream, _from, state) do
    {:reply, Pockets.keys_stream(__MODULE__), state}
  end

  def handle_call({:merge, data}, _from, state) do
    Pockets.merge(__MODULE__, data)
    {:reply, :ok, state}
  end

  def handle_call({:put, key, value}, _from, state) do
    Pockets.put(__MODULE__, key, value)
    {:reply, :ok, state}
  end

  def handle_call(:to_map, _from, state) do
    {:reply, Pockets.to_map(__MODULE__), state}
  end

  def handle_call(:to_stream, _from, state) do
    {:reply, Pockets.to_stream(__MODULE__), state}
  end
end
