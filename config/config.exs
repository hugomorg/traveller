import Config

config :traveller, ecto_repos: [Traveller.TestRepo]

config :traveller, Traveller.TestRepo,
  username: "postgres",
  password: "postgres",
  database: "traveller_test",
  hostname: "localhost",
  port: 5432,
  pool: Ecto.Adapters.SQL.Sandbox
