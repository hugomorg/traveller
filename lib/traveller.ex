defmodule Traveller do
  import Ecto.Query

  def start_stream(repo, schema, opts \\ []) do
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
            {direction, first_field} =
              case cursor do
                [{direction, field} | _] -> {direction, field}
                [field | _] -> {:asc, field}
                {direction, field} -> {direction, field}
                field -> {:asc, field}
              end

            type = schema.__schema__(:type, first_field)

            case {direction, type} do
              {:asc, type} when type in [:id, :integer] ->
                0

              {:asc, :string} ->
                ""

              {:desc, _} ->
                # We do not know what the upper bound should be, so raise
                raise "You must provide a start_after value for a desc ordering for #{first_field}"
            end
          end)

        next_cursor =
          Keyword.get(opts, :next_cursor, fn results ->
            cursor =
              case cursor do
                {_, cursor} -> cursor
                cursor -> cursor
              end

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

  defp iterate(params = %{cursor: cursor})
       when is_atom(cursor) do
    params
    |> Map.update!(:cursor, &{:asc, &1})
    |> iterate
  end

  defp iterate(
         params = %{
           chunk_size: chunk_size,
           cursor: {:asc, cursor},
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
           cursor: {:desc, cursor},
           next_cursor: next_cursor,
           repo: repo,
           schema: schema,
           start_after: start_after,
           mode: :cursor
         }
       ) do
    schema
    |> where([s], field(s, ^cursor) < ^start_after)
    |> order_by(desc: ^cursor)
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
           cursor: cursor,
           next_cursor: next_cursor,
           repo: repo,
           schema: schema,
           start_after: start_after,
           mode: :cursor
         }
       )
       when is_list(cursor) do
    unless length(start_after) == length(cursor) do
      raise "Cursor length must match cursor fields"
    end

    {_, query} =
      cursor
      |> Enum.zip(start_after)
      |> Enum.reduce(schema, &build_comparison_query/2)

    query
    |> order_by(^cursor)
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

  # If multi-field cursor, and first n fields are equal, keep comparing
  defp build_comparison_query({{:asc, field}, cursor}, {{prev_field, prev_cursor}, query}) do
    {{field, cursor},
     or_where(
       query,
       [s],
       field(s, ^prev_field) == ^prev_cursor and field(s, ^field) > ^cursor
     )}
  end

  defp build_comparison_query({{:desc, field}, cursor}, {{prev_field, prev_cursor}, query}) do
    {{field, cursor},
     or_where(
       query,
       [s],
       field(s, ^prev_field) == ^prev_cursor and field(s, ^field) < ^cursor
     )}
  end

  # Assume asc sort if not specified
  defp build_comparison_query({field, cursor}, {{prev_field, prev_cursor}, query}) do
    build_comparison_query({{:asc, field}, cursor}, {{prev_field, prev_cursor}, query})
  end

  defp build_comparison_query({{:desc, field}, cursor}, query) do
    {{field, cursor}, or_where(query, [s], field(s, ^field) < ^cursor)}
  end

  defp build_comparison_query({{:asc, field}, cursor}, query) do
    {{field, cursor}, or_where(query, [s], field(s, ^field) > ^cursor)}
  end

  defp build_comparison_query({field, cursor}, query) do
    build_comparison_query({{:asc, field}, cursor}, query)
  end
end
