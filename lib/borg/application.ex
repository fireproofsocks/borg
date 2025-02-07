defmodule Borg.Application do
  @moduledoc false

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    # IO.inspect(type, label: "start type")

    children = [
      pg_spec(),
      {Cluster.Supervisor,
       [
         Application.fetch_env!(:libcluster, :topologies),
         [name: Borg.ClusterSupervisor]
       ]},
      Borg.Foo,
      Borg.Collective,
      Borg.Storage

      # Borg.Notify
    ]

    opts = [strategy: :one_for_one, name: Borg.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # defp pg_spec do
  #   %{
  #     id: :pg,
  #     start: {:pg, :start_link, [:foobar]}
  #   }
  # end

  defp pg_spec do
    %{
      id: :pg,
      start: {:pg, :start_link, []}
    }
  end

  # @impl true
  # def start_phase(:notify, :normal, opts) do
  #   IO.puts("WHUT WHUT WHUT........................:)")
  #   pids = :pg.get_members(:borg_collective)
  #   Logger.debug("START PHASE... PIDS in the group are #{inspect(pids)}")
  #   :ok
  # end
end
