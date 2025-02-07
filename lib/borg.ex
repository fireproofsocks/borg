defmodule Borg do
  @moduledoc """
  Public functions for accessing `Borg` stuff
  """
  require Logger

  @redundancy 2

  @doc """
  Retrieves the value at the given key or returns an error tuple. Looks for the value
  on up to #{@redundancy} nodes.
  """
  @spec get(key :: term()) :: {:ok, any()} | {:error, String.t()}
  def get(key) do
    fail_msg = {:error, "Key #{inspect(key)} not found"}

    key
    |> whereis()
    |> Enum.reduce_while(fail_msg, fn node, acc ->
      # we don't have a fetch operation, so we look for `nil`s
      case Borg.Storage.get({Borg.Storage, node}, key) do
        nil ->
          {:cont, acc}

        value ->
          {:halt, {:ok, value}}
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
      Logger.debug("Writing key #{inspect(key)} to nodes #{inspect(owners)}")

      owners
      |> Enum.map(fn node ->
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
  def whereis(ring \\ Borg.Collective.ring(), key, redundancy \\ @redundancy) do
    HashRing.key_to_nodes(ring, key, redundancy)
  end

  @doc """
  Prints info about the cluster and keys etc, for use in iex
  """
  def info do
    ring = Borg.Collective.ring()
    nodes = HashRing.nodes(ring)

    rows =
      Enum.map(nodes, fn node ->
        [node, Borg.Storage.info({Borg.Storage, node}).size]
      end)

    Cowrie.table(rows, ["Node", "Key Cnt"])
  end

  @doc """
  Dumps local storage
  """
  def local do
    Borg.Storage.to_map(Borg.Storage)
  end
end
