defmodule Borg.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Cluster.Supervisor,
       [
         Application.fetch_env!(:libcluster, :topologies),
         [name: Borg.ClusterSupervisor]
       ]},
      Borg.Balancer,
      Borg.Storage
    ]

    opts = [strategy: :one_for_one, name: Borg.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
