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
            {direction, cursor} =
              case cursor do
                [_ | _] = list -> {:maybe_desc, list}
                {direction, field} -> {direction, field}
                field -> {:asc, field}
              end

            determine_start_after(schema, {direction, cursor})
          end)

        next_cursor =
          Keyword.get_lazy(opts, :next_cursor, fn ->
            next_cursor_fun =
              case cursor do
                cursor when is_list(cursor) ->
                  fn last ->
                    Enum.map(cursor, &Map.fetch!(last, &1))
                  end

                {_, cursor} ->
                  &Map.fetch!(&1, cursor)

                cursor ->
                  &Map.fetch!(&1, cursor)
              end

            fn results ->
              results
              |> List.last()
              |> next_cursor_fun.()
            end
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
        if length(results) == chunk_size do
          {results, %{params | offset: offset + chunk_size}}
        else
          {results, :done}
        end
    end
  end

  defp iterate(:done) do
    nil
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

  defp determine_start_after(schema, {:asc, field}) do
    case schema.__schema__(:type, field) do
      type when type in [:id, :integer, :float, :decimal] ->
        0

      :string ->
        ""

      type ->
        raise "We can't determine an appropriate start value for type #{type} for field #{field}"
    end
  end

  defp determine_start_after(schema, {:maybe_desc, list}) when is_list(list) do
    Enum.map(list, fn maybe_field ->
      parsed_field =
        case maybe_field do
          {:desc, field} ->
            # We do not know what the upper bound should be, so raise
            raise "You must provide a start_after value for a desc ordering for field #{field}"

          {:asc, field} ->
            field

          field ->
            field
        end

      determine_start_after(schema, {:asc, parsed_field})
    end)
  end

  defp determine_start_after(_schema, {:desc, field}) do
    raise "You must provide a start_after value for a desc ordering for field #{field}"
  end
end
