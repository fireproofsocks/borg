defmodule BorgTest do
  use ExUnit.Case

  describe "put/2" do
    test ":ok" do
      x = LocalCluster.start_link(3, prefix: "test-")
      dbg(x)
      assert {:ok, _} = Borg.put(:foo, "bar")
    end

    test ":error when not enough nodes available" do
      assert {:ok, _} = Borg.put(:foo, "bar")
    end
  end
end
