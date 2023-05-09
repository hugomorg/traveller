defmodule Traveller.RepoCase do
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox

  using do
    quote do
      import Ecto
      import Ecto.Changeset
      import Ecto.Query

      alias Traveller.TestRepo
      alias Traveller.Schemas.Person
    end
  end

  setup tags do
    :ok = Sandbox.checkout(Traveller.TestRepo)

    unless tags[:async] do
      Sandbox.mode(Traveller.TestRepo, {:shared, self()})
    end

    :ok
  end
end
