defmodule Borg.Storage do
  @moduledoc """
  This module sets up a `GenServer` in front of an ETS table (using `Pockets` for
  an easier interface).  We aren't using a `GenServer` to maintain state, only
  to handle messages sent from other nodes and serialize the operations.

  Conceptually, the storage is a `Map` (i.e. a key/value store), but because it
  uses ETS we can stream output and keys, so we are less likely to blow up our
  memory when we have to rebalance data across nodes.

  Storage can be accessed locally

      iex> Borg.Storage.get(Borg.Storage, :some_key)

  or by other nodes if they include a more fully qualified process identifier, e.g.

      iex> Borg.Storage.get({Borg.Storage, :b@localhost}, :some_key)
  """
  use GenServer

  require Logger

  @doc """
  Starts up an instance of storage with an ETS table backing it.

  ## Options

  - `:name` identifies the process and the name of the underlying ETS table.
    Name should be restricted to  often an atom.  Default: `#{__MODULE__}`
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, name, name: name)
  end

  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  @doc """
  bulk delete
  """
  def drop(pid, keys) when is_list(keys) do
    GenServer.call(pid, {:drop, keys})
  end

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  def info(pid) do
    GenServer.call(pid, :info)
  end

  def merge(pid, %{} = data) do
    GenServer.call(pid, {:merge, data})
  end

  def put(pid, key, value) do
    GenServer.call(pid, {:put, key, value})
  end

  @doc """
  Returns data as a stream
  """
  def to_stream(pid) do
    GenServer.call(pid, :to_stream)
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
  def init(name), do: Pockets.new(name)

  @impl true
  def handle_call({:delete, key}, _from, name) do
    Logger.debug("Deleting key #{key} from node #{node()}.")
    Pockets.delete(name, key)
    {:reply, :ok, name}
  end

  def handle_call({:drop, keys}, _from, name) do
    Logger.debug("Droping #{length(keys)} keys from node #{node()}.")
    key_set = MapSet.new(keys)
    # Warning: this updates the table in place
    {:ok, _keys_dropped} = Pockets.reject(name, fn {k, _v} -> MapSet.member?(key_set, k) end)
    {:reply, :ok, name}
  end

  def handle_call({:get, key}, _from, name) do
    {:reply, Pockets.get(name, key), name}
  end

  def handle_call(:info, _from, name) do
    {:reply, Pockets.info(name), name}
  end

  def handle_call({:merge, data}, _from, name) do
    Pockets.merge(name, data)
    {:reply, :ok, name}
  end

  def handle_call({:put, key, value}, _from, name) do
    Pockets.put(name, key, value)
    {:reply, :ok, name}
  end

  def handle_call(:to_map, _from, name) do
    {:reply, Pockets.to_map(name), name}
  end

  def handle_call(:to_stream, _from, name) do
    {:reply, Pockets.to_stream(name), name}
  end
end
