defmodule Borg.StorageTest do
  use ExUnit.Case
  alias Borg.Storage

  describe "start_link/1" do
    test ":ok", %{test: test} do
      assert {:ok, _} = Storage.start_link(name: test)
    end

    test ":error when names are not unique", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      assert {:error, _} = Storage.start_link(name: test)
    end
  end

  describe "delete/2" do
    test ":ok", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      :ok = Storage.put(test, :test, test)
      assert :ok = Storage.delete(test, :test)
      assert nil == Storage.get(test, :test)
    end
  end

  describe "drop/2" do
    test ":ok", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      :ok = Storage.put(test, :k1, test)
      :ok = Storage.put(test, :k2, test)
      :ok = Storage.put(test, :k3, test)
      assert :ok = Storage.drop(test, [:k1, :k2, :k3])
      assert nil == Storage.get(test, :k1)
      assert nil == Storage.get(test, :k2)
      assert nil == Storage.get(test, :k3)
    end
  end

  describe "get/2" do
    test ":ok", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      :ok = Storage.put(test, :k1, test)
      assert ^test = Storage.get(test, :k1)
    end

    test "nil when key does not exist", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      assert nil == Storage.get(test, test)
    end
  end

  describe "info/1" do
    test "gets info", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      assert is_map(Storage.info(test))
    end
  end

  describe "merge/2" do
    test ":ok", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      assert :ok = Storage.merge(test, %{k1: test, k2: test, k3: test})
      assert %{k1: ^test, k2: ^test, k3: ^test} = Storage.to_map(test)
    end
  end

  describe "put/3" do
    test ":ok", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      assert :ok = Storage.put(test, :k1, test)
      assert ^test = Storage.get(test, :k1)
    end
  end

  describe "to_stream/1" do
    test "is function", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      :ok = Storage.put(test, :k1, test)
      :ok = Storage.put(test, :k2, test)
      :ok = Storage.put(test, :k3, test)

      assert is_function(Storage.to_stream(test))
    end
  end

  describe "to_map/1" do
    test "is function", %{test: test} do
      {:ok, _} = Storage.start_link(name: test)
      :ok = Storage.put(test, :k1, test)
      :ok = Storage.put(test, :k2, test)
      :ok = Storage.put(test, :k3, test)

      assert %{k1: ^test, k2: ^test, k3: ^test} = Storage.to_map(test)
    end
  end
end
