defmodule Borg.Application do
  @moduledoc false

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    children = [
      pg_spec(),
      {Cluster.Supervisor,
       [
         Application.fetch_env!(:libcluster, :topologies),
         [name: Borg.ClusterSupervisor]
       ]},
      Borg.Collective,
      Borg.Storage
    ]

    opts = [strategy: :one_for_one, name: Borg.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp pg_spec do
    %{
      id: :pg,
      start: {:pg, :start_link, []}
    }
  end
end
