defmodule TravellerTest do
  use Traveller.RepoCase
  alias Traveller.Person
  alias Traveller.TestRepo

  setup do
    albus_dumbledore =
      TestRepo.insert!(%Person{first_name: "Albus", last_name: "Dumbledore", age: 1001})

    severus_snape = TestRepo.insert!(%Person{first_name: "Severus", last_name: "Snape", age: 42})

    bruce_wayne = TestRepo.insert!(%Person{first_name: "Bruce", last_name: "Wayne", age: 37})

    %{
      albus_dumbledore: albus_dumbledore,
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape,
      sorted_by_id: [albus_dumbledore, severus_snape, bruce_wayne]
    }
  end

  describe "cursor mode" do
    test "default cursor is id", %{sorted_by_id: sorted_by_id} do
      stream = Traveller.run(repo: TestRepo, schema: Person)
      assert Enum.to_list(stream) == [sorted_by_id]
    end

    test "chunk size is configurable", %{
      albus_dumbledore: albus_dumbledore,
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape
    } do
      stream =
        Traveller.run(
          repo: TestRepo,
          schema: Person,
          chunk_size: 1,
          cursor: :first_name,
          start_after: ""
        )

      assert Enum.to_list(stream) == [[albus_dumbledore], [bruce_wayne], [severus_snape]]
    end

    test "start_after is configurable", %{
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape
    } do
      stream =
        Traveller.run(
          repo: TestRepo,
          schema: Person,
          start_after: bruce_wayne.first_name,
          cursor: :first_name
        )

      assert Enum.to_list(stream) == [[severus_snape]]
    end

    test "cursor field is configurable", %{
      albus_dumbledore: albus_dumbledore,
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape
    } do
      stream =
        Traveller.run(repo: TestRepo, schema: Person, cursor: :first_name, start_after: "A")

      assert Enum.to_list(stream) == [[albus_dumbledore, bruce_wayne, severus_snape]]
    end

    test "cursor can be a list of fields", %{
      albus_dumbledore: albus_dumbledore,
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape
    } do
      albus_bob = TestRepo.insert!(%Person{first_name: "Albus", last_name: "Bob"})

      stream =
        Traveller.run(
          repo: TestRepo,
          schema: Person,
          cursor: [:first_name, :last_name],
          start_after: [albus_bob.first_name, albus_bob.last_name],
          next_cursor: fn results ->
            last = List.last(results)
            [last.first_name, last.last_name]
          end,
          chunk_size: 1
        )

      assert Enum.to_list(stream) == [[albus_dumbledore], [bruce_wayne], [severus_snape]]
    end

    test "cursor can be a list of fields - any sort directions", %{
      albus_dumbledore: albus_dumbledore,
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape
    } do
      alice_wayne = TestRepo.insert!(%Person{first_name: "Alice", last_name: "Wayne"})
      lisa_wayne = TestRepo.insert!(%Person{first_name: "Lisa", last_name: "Wayne"})

      stream =
        Traveller.run(
          repo: TestRepo,
          schema: Person,
          cursor: [desc: :last_name, asc: :first_name],
          start_after: ["Z", "Z"],
          next_cursor: fn results ->
            last = List.last(results)
            [last.first_name, last.last_name]
          end
        )

      assert Enum.to_list(stream) == [
               [alice_wayne, bruce_wayne, lisa_wayne, severus_snape, albus_dumbledore]
             ]
    end

    test "cursor can be specified in desc order", %{
      albus_dumbledore: albus_dumbledore,
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape
    } do
      stream =
        Traveller.run(
          repo: TestRepo,
          schema: Person,
          cursor: {:desc, :first_name},
          start_after: "z"
        )

      assert Enum.to_list(stream) == [[severus_snape, bruce_wayne, albus_dumbledore]]
    end
  end

  describe "offset mode" do
    test "default fetches 100", %{sorted_by_id: sorted_by_id} do
      stream = Traveller.run(repo: TestRepo, schema: Person, mode: :offset)
      assert Enum.to_list(stream) == [sorted_by_id]
    end

    test "chunk size is configurable", %{
      albus_dumbledore: albus_dumbledore,
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape
    } do
      stream =
        Traveller.run(
          repo: TestRepo,
          schema: Person,
          mode: :offset,
          chunk_size: 2,
          sort_key: :first_name
        )

      assert Enum.to_list(stream) == [[albus_dumbledore, bruce_wayne], [severus_snape]]
    end

    test "initial_offset is configurable", %{
      severus_snape: severus_snape
    } do
      stream =
        Traveller.run(
          repo: TestRepo,
          schema: Person,
          mode: :offset,
          initial_offset: 2,
          sort_key: :first_name
        )

      assert Enum.to_list(stream) == [[severus_snape]]
    end

    test "sort_key is configurable", %{
      albus_dumbledore: albus_dumbledore,
      bruce_wayne: bruce_wayne,
      severus_snape: severus_snape
    } do
      stream = Traveller.run(repo: TestRepo, schema: Person, mode: :offset, sort_key: :last_name)
      assert Enum.to_list(stream) == [[albus_dumbledore, severus_snape, bruce_wayne]]
    end
  end
end
