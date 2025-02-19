defmodule Borg do
  @moduledoc """
  Welcome to `Borg`! This is a proof-of-concept application that demonstrates some
  of the functions and techniques for working with distributed Elixir applications.
  There is no master: every instance of this application is a peer.
  See the README for examples of how to kick the tires.
  """
  alias Borg.Collective
  alias Borg.Storage

  require Logger

  @redundancy 2

  @doc """
  Retrieves the value at the given key or returns an error tuple. Looks for the value
  on up to #{@redundancy} nodes.
  FYI: although our Storage implementation is a get (with optional default), here
  we implement a fetch operation with an :ok|:error tuple returned
  """
  @spec fetch(key :: term()) :: {:ok, any()} | {:error, String.t()}
  def fetch(key) do
    fail_msg = {:error, "Key #{inspect(key)} not found"}

    key
    |> whereis()
    |> Enum.reduce_while(fail_msg, fn node, acc ->
      # we don't have a fetch operation, so we look for `nil`s
      case Storage.get({Storage, node}, key) do
        nil ->
          {:cont, acc}

        value ->
          {:halt, {:ok, value}}
      end
    end)
  end

  @doc """
  Returns info about the cluster and key distribution
  """
  def info do
    # https://hexdocs.pm/elixir/GenServer.html#multi_call/4
    {replies, _bad_nodes} =
      GenServer.multi_call(MapSet.to_list(Collective.members()), Storage, :info)

    Enum.map(replies, fn {node, info_struct} ->
      %{node: node, key_count: info_struct.size}
    end)
  end

  @doc """
  Dumps local storage
  """
  def local do
    Storage.to_map(Storage)
  end

  @doc """
  Puts the key/value into storage on one or more of the other nodes in the cluster
  so data exists on #{@redundancy} nodes.
  """
  @spec put(key :: term(), value :: any()) :: :ok | {:error, String.t()}
  def put(key, value) do
    with {:ok, target_nodes} <- get_target_nodes(key),
         :ok <- ensure_safe_to_write(target_nodes) do
      multi_put(target_nodes, key, value)
    end
  end

  # Enforces redundancy rule
  defp get_target_nodes(key) do
    nodes = whereis(key)

    if length(nodes) >= @redundancy do
      {:ok, nodes}
    else
      {:error, "Write operations require at least #{@redundancy} nodes to be available."}
    end
  end

  # there isn't an easy way to see if remote processes are running
  # See note https://hexdocs.pm/elixir/Process.html#alive?/1
  defp ensure_safe_to_write(nodes) do
    nodes
    |> Enum.all?(fn node -> :erpc.call(node, Process, :whereis, [Borg.Rebalancer]) == nil end)
    |> case do
      true -> :ok
      false -> {:error, "One or more nodes is rebalancing data. Try again later"}
    end
  end

  defp multi_put(nodes, key, value) do
    Logger.debug("Writing key #{inspect(key)} to nodes #{inspect(nodes)}")
    {replies, _bad_nodes} = GenServer.multi_call(nodes, Storage, {:put, key, value})

    replies
    |> Enum.all?(fn {_node, response} -> response == :ok end)
    |> case do
      true -> :ok
      false -> {:error, "Error writing to one or more nodes"}
    end
  end

  @doc """
  Returns the node(s) which own the given key given the expected redundancy.
  The results are ordered.
  """
  @spec whereis(node_set :: MapSet.t(), key :: term(), redundancy :: non_neg_integer()) :: list()
  def whereis(node_set \\ Borg.Collective.members(), key, redundancy \\ @redundancy) do
    # See https://elixirforum.com/t/unexpected-behavior-from-libring-hashring-unlucky-number-14/69333/6
    node_set
    |> Enum.sort_by(fn n -> {:erlang.phash2({key, n}), n} end)
    |> Enum.take(redundancy)
  end
end
