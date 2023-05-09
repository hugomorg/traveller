defmodule TravellerTest do
  use Traveller.RepoCase
  alias Traveller.Person
  alias Traveller.TestRepo

  setup do
    dumbledore =
      TestRepo.insert!(%Person{first_name: "Albus", last_name: "Dumbledore", age: 1001})

    snape = TestRepo.insert!(%Person{first_name: "Severus", last_name: "Snape", age: 42})

    indiana_jones = TestRepo.insert!(%Person{first_name: "Indiana", last_name: "Jones", age: 59})

    batman = TestRepo.insert!(%Person{first_name: "Bruce", last_name: "Wayne", age: 37})

    %{
      dumbledore: dumbledore,
      snape: snape,
      indiana_jones: indiana_jones,
      batman: batman,
      all: [dumbledore, snape, indiana_jones, batman]
    }
  end

  describe "cursor mode" do
    test "default cursor is id", %{all: all} do
      stream = Traveller.run(repo: TestRepo, schema: Person)
      assert Enum.take(stream, 1) == [all]
    end

    test "chunk size is configurable", %{
      dumbledore: dumbledore,
      snape: snape,
      indiana_jones: indiana_jones,
      batman: batman
    } do
      stream = Traveller.run(repo: TestRepo, schema: Person, chunk_size: 1)
      assert Enum.take(stream, 1) == [[dumbledore]]
      assert Enum.take(stream, 2) == [[dumbledore], [snape]]
      assert Enum.take(stream, 3) == [[dumbledore], [snape], [indiana_jones]]
      assert Enum.take(stream, 4) == [[dumbledore], [snape], [indiana_jones], [batman]]
    end

    test "start_after is configurable", %{
      indiana_jones: indiana_jones,
      batman: batman
    } do
      stream = Traveller.run(repo: TestRepo, schema: Person, start_after: indiana_jones.id)
      assert Enum.take(stream, 1) == [[batman]]
    end

    test "cursor field is configurable", %{
      indiana_jones: indiana_jones,
      dumbledore: dumbledore,
      snape: snape,
      batman: batman
    } do
      stream =
        Traveller.run(repo: TestRepo, schema: Person, cursor: :first_name, start_after: "A")

      assert Enum.take(stream, 1) == [[dumbledore, batman, indiana_jones, snape]]
    end
  end

  describe "offset mode" do
    test "default fetches 100", %{all: all} do
      stream = Traveller.run(repo: TestRepo, schema: Person, mode: :offset)
      assert Enum.take(stream, 1) == [all]
    end

    test "chunk size is configurable", %{
      snape: snape,
      dumbledore: dumbledore,
      batman: batman,
      indiana_jones: indiana_jones
    } do
      stream = Traveller.run(repo: TestRepo, schema: Person, mode: :offset, chunk_size: 2)
      assert Enum.take(stream, 2) == [[dumbledore, snape], [indiana_jones, batman]]
    end

    test "initial_offset is configurable", %{
      snape: snape,
      dumbledore: dumbledore,
      batman: batman,
      indiana_jones: indiana_jones
    } do
      stream = Traveller.run(repo: TestRepo, schema: Person, mode: :offset, initial_offset: 2)
      assert Enum.take(stream, 1) == [[indiana_jones, batman]]
    end

    test "sort_key is configurable", %{
      snape: snape,
      dumbledore: dumbledore,
      batman: batman,
      indiana_jones: indiana_jones
    } do
      stream = Traveller.run(repo: TestRepo, schema: Person, mode: :offset, sort_key: :last_name)
      assert Enum.take(stream, 1) == [[dumbledore, indiana_jones, snape, batman]]
    end
  end
end
