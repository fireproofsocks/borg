defmodule Borg.Foo do
  use GenServer

  require Logger

  # :pg.start_link()
  # :pg.join(:my_group, Borg.Foo)
  # :pg.join(:my_group, Process.whereis(Borg.Foo))
  # :pg.get_members(:my_group)
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # GenServer callbacks
  def init(_) do
    Logger.debug("Initing Foo")
    state = %{}
    {:ok, state}
  end

  # def join_collective(node) do
  #   GenServer.call(__MODULE__, "Some message")
  # end

  def handle_call(msg, _from, state) do
    Logger.debug("Handling call  on node #{node()}: #{inspect(msg)}")
    {:reply, :ok, state}
  end

  def handle_cast(msg, _from, state) do
    Logger.debug("Handling cast  on node #{node()}: #{inspect(msg)}")
    {:noreply, state}
  end

  def handle_info(msg, _from, state) do
    Logger.debug("Handling info on node #{node()}: #{inspect(msg)}")
    {:noreply, state}
  end
end
