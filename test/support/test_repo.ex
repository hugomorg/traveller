defmodule Traveller.TestRepo do
  use Ecto.Repo,
    otp_app: :traveller,
    adapter: Ecto.Adapters.Postgres
end
