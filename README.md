# Borg

A simple app designed to explore some of the possibilities available to distributed Elixir.

This app implements a simple key/value store (think `Map`), but it does it in a distributed way so that data is distributed across all nodes in cluster and data is always written to two nodes. If a node goes down, data is rebalanced to other nodes, so the cluster should maintain integrity if any single node goes down.

Data is also rebalanced when a new node is detected in the cluster.

To see this in action, you should start up multiple instances of this application.  You must specify a short name via the `--sname` option.  For example, open 4 separate terminal tabs and start an instance of the app in each:

```sh
iex --sname a@localhost -S mix
iex --sname b@localhost -S mix
iex --sname c@localhost -S mix
iex --sname d@localhost -S mix
```

Next, in any of the running `iex` instances, try adding data:

```elixir
iex> Borg.put(:foo, "bar")
```

You should see some debug messages indicating that the value was written to 2 different nodes.

Add some data to your cluster:

```elixir
Enum.each(1..3, fn n -> Borg.put(n, n) end)
Enum.each(1..10, fn n -> Borg.put(n, n) end)
Enum.each(1..20, fn n -> Borg.put(n, n) end)
# or go big!
Enum.each(1..1000, fn n -> Borg.put(n, n) end)
```
