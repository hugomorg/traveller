defmodule Traveller.Car do
  use Ecto.Schema

  @primary_key false
  schema "cars" do
    field(:company, :string)
    field(:model, :string)
    field(:id, :integer)
  end
end
