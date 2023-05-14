defmodule Traveller.TestRepo.Migrations.CreateCarTable do
  use Ecto.Migration

  def change do
    create table :cars, primary_key: false do
      add :company, :string
      add :model, :string
      add :id, :integer
    end
  end
end
