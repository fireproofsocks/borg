defmodule Borg.CollectiveTest do
  use ExUnit.Case
  alias Borg.Collective

  describe "members/1" do
    test "gets current members of the collective" do
      assert %MapSet{} = Collective.members()
    end

    test "detects other Borg in cluster" do
      {:ok, _pid} = LocalCluster.start_link(3, prefix: "test-")
      Process.sleep(150)
      members = Collective.members()

      assert MapSet.size(members) == 4
    end
  end
end
