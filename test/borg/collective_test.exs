defmodule Borg.CollectiveTest do
  use ExUnit.Case
  alias Borg.Collective

  describe "ring/1" do
    test "gets current ring" do
      assert %HashRing{} = Collective.ring()
    end

    test "detects other Borg in cluster" do
      {:ok, _pid} = LocalCluster.start_link(3, prefix: "test-")
      Process.sleep(150)
      ring = Collective.ring()
      nodes = HashRing.nodes(ring)
      assert length(nodes) == 4
    end
  end
end
