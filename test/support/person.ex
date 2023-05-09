defmodule Traveller.Person do
  use Ecto.Schema

  schema "people" do
    field(:first_name, :string)
    field(:last_name, :string)
    field(:age, :integer)

    timestamps()
  end
end
