defmodule Traveller do
  import Ecto.Query

  def run(opts) do
    schema = Keyword.fetch!(opts, :schema)
    cursor = Keyword.get(opts, :cursor, :id)
    repo = Keyword.fetch!(opts, :repo)
    chunk_size = Keyword.get(opts, :chunk_size, 100)

    start_after =
      Keyword.get_lazy(opts, :start_after, fn ->
        # Make this smarter eventually
        0
      end)

    next_cursor =
      Keyword.get(opts, :next_cursor, fn results ->
        results |> List.last() |> Map.get(cursor)
      end)

    Stream.unfold(
      %{
        chunk_size: chunk_size,
        cursor: cursor,
        next_cursor: next_cursor,
        repo: repo,
        schema: schema,
        start_after: start_after
      },
      &iterate/1
    )
  end

  defp iterate(
         params = %{
           chunk_size: chunk_size,
           cursor: cursor,
           next_cursor: next_cursor,
           repo: repo,
           schema: schema,
           start_after: start_after
         }
       ) do
    schema
    |> where([s], field(s, ^cursor) > ^start_after)
    |> order_by(asc: ^cursor)
    |> limit(^chunk_size)
    |> repo.all()
    |> case do
      [] ->
        nil

      results ->
        {results, %{params | start_after: next_cursor.(results)}}
    end
  end
end
