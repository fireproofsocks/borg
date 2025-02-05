# Runtime configuration
import Config
import Dotenvy

# For local development, read dotenv files inside the envs/ dir;
# for releases, read them at the RELEASE_ROOT
env_dir_prefix = System.get_env("RELEASE_ROOT") || Path.expand("./envs/") <> "/"

source!([
  "#{env_dir_prefix}.env",
  "#{env_dir_prefix}.#{config_env()}.env",
  "#{env_dir_prefix}.#{config_env()}.overrides.env",
  System.get_env()
])

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
