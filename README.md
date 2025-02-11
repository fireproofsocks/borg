# Borg

A simple app designed to explore some of the possibilities available to distributed Elixir.

This app implements a simple key/value store (think `Map`), but it does it in a distributed way so that data is distributed across all nodes in cluster and data is always written to **two nodes**.  The cluster should maintain integrity if any single node goes down because data must always exist in two places.

All members of the cluster are peers: there is no leader.  There should be no single point of failure.

Likewise, data is rebalanced when a new node is detected in the cluster.

To see this in action, you should start up multiple instances of this application.  You must specify a short name via the `--sname` option.  For example, open 2 separate terminal tabs and start an instance of the app in each:

```sh
iex --sname a@localhost -S mix
iex --sname b@localhost -S mix
```

Next, in any of the running `iex` instances, try adding data:

```elixir
Enum.each(1..3, fn n -> Borg.put(n, n) end)
Enum.each(1..10, fn n -> Borg.put(n, n) end)
Enum.each(1..20, fn n -> Borg.put(n, n) end)
# or go big!
Enum.each(1..200, fn n -> Borg.put(n, n) end)
Enum.each(1..1000, fn n -> Borg.put(n, n) end)
```

You should see some debug messages indicating that the value was written to 2 different nodes.  In any node, you can inspect the distribution:

```elixir
iex> Borg.info()
[
  %{node: :c@localhost, key_count: 675},
  %{node: :a@localhost, key_count: 657},
  %{node: :b@localhost, key_count: 668}
]
```

If you now add another node, data should be rebalanced so it is distributed _somewhat_ evenly across the nodes.

```sh
iex --sname c@localhost -S mix
iex --sname d@localhost -S mix
```

## Benchmarking

Writing is a little slow. For example, writing 1 million keys via `Enum.each(1..1000000, fn n -> Borg.put(n, n) end)` might take a minute or two.

However, rebalancing is relatively fast.  Adding a 3rd node to a cluster of 2 nodes containing a million keys took about 3 seconds on an M1 Mac with 10 cores.

Some efficiency could be gained by leveraging `GenServer.multi_call/4` inside `Borg.put/2`...

## See Also

Some articles and references which helped me piece this together:

- <https://bigardone.dev/blog/2021/06/06/three-real-world-examples-of-distributed-elixir-pt-2>
- <https://whitfin.io/blog/setting-up-distributed-nodes-in-elixir-unit-tests/>
- <https://stackoverflow.com/questions/67957826/what-is-the-correct-way-to-start-pgs-default-scope-in-an-elixir-1-12-applica>
- <https://papers.vincy.dev/distributed-pubsub-in-elixir>
- <https://learnyousomeerlang.com/distributed-otp-applications>
- <https://hexdocs.pm/horde/readme.html>
- <https://en.wikipedia.org/wiki/Consistent_hashing>
- <https://www.tzeyiing.com/posts/erlang-global-resource-locks-elixir/>
- <https://elixirforum.com/t/unexpected-behavior-from-libring-hashring-unlucky-number-14/69333/4>
- <https://en.wikipedia.org/wiki/Rendezvous_hashing>
- <https://www.usenix.org/system/files/conference/atc13/atc13-cidon.pdf>
- <https://github.com/derekkraan/horde/issues/277>
