defmodule Borg.RebalancerTest do
  use ExUnit.Case
  alias Borg.Rebalancer

  describe "get_allocation/3" do
    test ":ok" do
      nodes = MapSet.new(["a", "b", "c"])

      chunk = Enum.map(1..10, fn n -> {n, n} end) |> Map.new()

      assert {
               %{
                 "b" => %{2 => 2, 3 => 3, 6 => 6, 1 => 1, 4 => 4, 5 => 5, 9 => 9},
                 "c" => %{3 => 3, 4 => 4, 7 => 7, 8 => 8, 9 => 9, 10 => 10, 6 => 6}
               },
               [9, 6, 4, 3]
             } = Rebalancer.get_allocation(chunk, nodes, "a")
    end
  end
end
