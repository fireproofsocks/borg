defmodule Borg.MixProject do
  use Mix.Project

  def project do
    [
      app: :borg,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Borg.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:libcluster, "~> 3.5"},
      {:local_cluster, "~> 2.1", only: :test},
      # {:cowrie, "~> 0.4.0"},
      {:pockets, "~> 1.6.0"}
    ]
  end
end
