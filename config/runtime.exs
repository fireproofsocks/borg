# Runtime configuration
import Config

config :libcluster,
  topologies: [
    borg: [
      strategy: Cluster.Strategy.Gossip,
      config: [
        port: 45892,
        broadcast_only: false,
        secret: "WE-ARE-BORG"
      ]
    ]
  ]
