defmodule Traveller do
  import Ecto.Query

  def run(opts) do
    schema = Keyword.fetch!(opts, :schema)
    repo = Keyword.fetch!(opts, :repo)
    chunk_size = Keyword.get(opts, :chunk_size, 100)
    mode = Keyword.get(opts, :mode, :cursor)

    base_opts = %{schema: schema, repo: repo, chunk_size: chunk_size, mode: mode}

    opts =
      if mode == :offset do
        Map.merge(base_opts, %{
          offset: Keyword.get(opts, :initial_offset, 0),
          sort_key: Keyword.get(opts, :sort_key, :id)
        })
      else
        cursor = Keyword.get(opts, :cursor, :id)

        start_after =
          Keyword.get_lazy(opts, :start_after, fn ->
            # Make this smarter eventually
            0
          end)

        next_cursor =
          Keyword.get(opts, :next_cursor, fn results ->
            results |> List.last() |> Map.get(cursor)
          end)

        Map.merge(base_opts, %{
          cursor: cursor,
          start_after: start_after,
          next_cursor: next_cursor
        })
      end

    Stream.unfold(opts, &iterate/1)
  end

  defp iterate(
         params = %{
           chunk_size: chunk_size,
           cursor: cursor,
           next_cursor: next_cursor,
           repo: repo,
           schema: schema,
           start_after: start_after,
           mode: :cursor
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

  defp iterate(
         params = %{
           chunk_size: chunk_size,
           repo: repo,
           schema: schema,
           offset: offset,
           mode: :offset,
           sort_key: sort_key
         }
       ) do
    schema
    |> order_by(asc: ^sort_key)
    |> limit(^chunk_size)
    |> offset(^offset)
    |> repo.all()
    |> case do
      [] ->
        nil

      results ->
        {results, %{params | offset: offset + chunk_size}}
    end
  end
end
