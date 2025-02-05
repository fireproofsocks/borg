defmodule Borg do
  @moduledoc """
  Documentation for `Borg`.
  """
  require Logger

  @redundancy 2

  @doc """
  Retrieves the value at the given key or returns an error tuple. Looks for the value
  on up to #{@redundancy} nodes.
  """
  @spec fetch(key :: term()) :: {:ok, any()} | {:error, String.t()}
  def fetch(key) do
    fail_msg = {:error, "Key #{inspect(key)} not found"}

    key
    |> whereis()
    |> Enum.reduce_while(fail_msg, fn node, acc ->
      case Borg.Storage.fetch({Borg.Storage, node}, key) do
        {:ok, value} ->
          {:halt, {:ok, value}}

        :error ->
          {:cont, acc}
      end
    end)
  end

  @doc """
  Puts the key/value into storage on one or more of the other nodes in the cluster
  so data exists on #{@redundancy} nodes.
  """
  def put(key, value) do
    owners = whereis(key)

    if length(owners) < @redundancy do
      {:error, "Write operations require at least #{@redundancy} nodes to be available."}
    else
      owners
      |> Enum.map(fn node ->
        Logger.debug("Writing key #{inspect(key)} to node #{inspect(node)}")
        Borg.Storage.put({Borg.Storage, node}, key, value)
      end)
      |> Enum.all?(fn result -> result == :ok end)
      |> case do
        true -> :ok
        false -> {:error, "Error writing to one or more nodes"}
      end
    end
  end

  @doc """
  Indicates which node(s) own a the given key given the expected redundancy.
  """
  def whereis(ring \\ Borg.Balancer.ring(), key, redundancy \\ @redundancy) do
    HashRing.key_to_nodes(ring, key, redundancy)
  end

  @doc """
  Prints info about the cluster and keys etc, for use in iex
  """
  def info do
    ring = Borg.Balancer.ring()
    nodes = HashRing.nodes(ring)

    rows =
      Enum.map(nodes, fn node ->
        [node, length(Borg.Storage.keys({Borg.Storage, node}))]
      end)

    Cowrie.table(rows, ["Node", "Key Cnt"])
  end

  @doc """
  Dumps local storage
  """
  def dump do
    Borg.Storage.state(Borg.Storage)
  end
end
