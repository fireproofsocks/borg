defmodule BorgTest do
  use ExUnit.Case, async: false

  setup do
    # Suppress info messages on all nodes
    level = Logger.level()
    Logger.configure(level: :warning)

    on_exit(fn ->
      Logger.configure(level: level)
    end)
  end

  describe "get/1" do
    test ":ok", %{test: test} do
      # {:ok, _pid} = LocalCluster.start_link(3, prefix: "#{test}")
      {:ok, pid} = LocalCluster.start_link(3, prefix: "test-get-")
      Process.sleep(150)
      :ok = Borg.put(:k1, test)
      assert {:ok, ^test} = Borg.get(:k1)
      LocalCluster.stop(pid)
    end
  end

  describe "put/2" do
    test ":ok", %{test: _test} do
      # {:ok, _pid} = LocalCluster.start_link(3, prefix: "#{test}")
      {:ok, pid} = LocalCluster.start_link(3, prefix: "test-put-")
      Process.sleep(150)
      assert :ok = Borg.put(:foo, "bar")
      LocalCluster.stop(pid)
    end

    test ":error when not enough nodes available" do
      assert {:error, _} = Borg.put(:foo, "bar")
    end
  end

  describe "whereis/3" do
    test "returns the proper number" do
      node_set = MapSet.new(["a", "b", "c"])

      assert [_node1, _node2] = Borg.whereis(node_set, "key", 2)
    end
  end
end
