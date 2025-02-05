defmodule BorgTest do
  use ExUnit.Case
  doctest Borg

  test "greets the world" do
    assert Borg.hello() == :world
  end
end
