defmodule Borg.MixProject do
  use Mix.Project

  def project do
    [
      app: :borg,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases()
    ]
  end

  defp releases do
    [
      borg: [
        overlays: ["envs/"]
      ]
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
      {:dotenvy, "~> 1.0.0"},
      {:libcluster, "~> 3.5"},
      {:libring, "~> 1.7"},
      {:local_cluster, "~> 2.1"},
      {:cowrie, "~> 0.4.0"},
      {:pockets, "~> 1.6.0"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
