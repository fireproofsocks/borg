defmodule Borg.RebalancerTest do
  use ExUnit.Case
  alias Borg.Rebalancer

  describe "redistribute/4" do
    # test ":ok" do
    #   # ring = HashRing.add_nodes(HashRing.new(), ["a", "b", "c"])
    #   # Rebalancer.calc_redistribution(kv_stream, ring, this_node, chunk_size)
    # end
  end

  describe "allocate_chunk/3" do
    test ":ok" do
      ring = HashRing.add_nodes(HashRing.new(), ["a", "b", "c"])

      chunk = Enum.map(1..10, fn n -> {n, n} end) |> Map.new()

      assert {%{
                "b" => %{2 => 2, 3 => 3, 6 => 6, 10 => 10},
                "c" => %{1 => 1, 3 => 3, 4 => 4, 5 => 5, 7 => 7, 8 => 8, 9 => 9, 10 => 10}
              }, [10, 3]} = Rebalancer.allocate_chunk(chunk, ring, "a")
    end
  end
end
