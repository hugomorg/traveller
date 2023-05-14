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
          order_by: Keyword.get(opts, :order_by, :id)
        })
      else
        cursor = Keyword.get(opts, :cursor, :id)

        ref = make_ref()

        # If no `start_after` is provided then we use a default
        # from the table. However since the cursor is exclusive we have
        # to specify the first query as inclusive, as if there was a virtual
        # cursor greater / lower than the one returned from here
        {inclusive, start_after} =
          opts
          |> Keyword.get_lazy(:start_after, fn ->
            {direction, field} =
              case cursor do
                [{direction, field} | _] -> {direction, field}
                [field | _] -> {:asc, field}
                {direction, field} -> {direction, field}
                field -> {:asc, field}
              end

            start_after = determine_start_after(repo, schema, {direction, field})
            {ref, start_after}
          end)
          |> case do
            {^ref, start_after} -> {true, start_after}
            start_after -> {false, start_after}
          end

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
          inclusive: inclusive,
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
    |> then(fn query ->
      if params[:inclusive] do
        where(query, [s], field(s, ^cursor) >= ^start_after)
      else
        where(query, [s], field(s, ^cursor) > ^start_after)
      end
    end)
    |> order_by(asc: ^cursor)
    |> limit(^chunk_size)
    |> repo.all()
    |> case do
      [] ->
        nil

      results ->
        {results, %{drop_inclusive(params) | start_after: next_cursor.(results)}}
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
    |> then(fn query ->
      if params[:inclusive] do
        where(query, [s], field(s, ^cursor) <= ^start_after)
      else
        where(query, [s], field(s, ^cursor) < ^start_after)
      end
    end)
    |> order_by(desc: ^cursor)
    |> limit(^chunk_size)
    |> repo.all()
    |> case do
      [] ->
        nil

      results ->
        {results, %{drop_inclusive(params) | start_after: next_cursor.(results)}}
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
      |> Enum.zip(List.wrap(start_after))
      |> Enum.reduce(schema, &build_comparison_query(&1, &2, params[:inclusive]))

    query
    |> order_by(^cursor)
    |> limit(^chunk_size)
    |> repo.all()
    |> case do
      [] ->
        nil

      results ->
        {results, %{drop_inclusive(params) | start_after: next_cursor.(results)}}
    end
  end

  defp iterate(params = %{mode: :offset, order_by: order_by}) when is_atom(order_by) do
    params
    |> Map.update!(:order_by, &{:asc, &1})
    |> iterate
  end

  defp iterate(
         params = %{
           chunk_size: chunk_size,
           repo: repo,
           schema: schema,
           offset: offset,
           mode: :offset,
           order_by: {direction, order_by}
         }
       ) do
    schema
    |> order_by([{^direction, ^order_by}])
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
  defp build_comparison_query({{:asc, field}, cursor}, {{prev_field, prev_cursor}, query}, _) do
    {{field, cursor},
     or_where(
       query,
       [s],
       field(s, ^prev_field) == ^prev_cursor and field(s, ^field) > ^cursor
     )}
  end

  defp build_comparison_query({{:desc, field}, cursor}, {{prev_field, prev_cursor}, query}, _) do
    {{field, cursor},
     or_where(
       query,
       [s],
       field(s, ^prev_field) == ^prev_cursor and field(s, ^field) < ^cursor
     )}
  end

  # Assume asc sort if not specified
  defp build_comparison_query({field, cursor}, {{prev_field, prev_cursor}, query}, inclusive) do
    build_comparison_query({{:asc, field}, cursor}, {{prev_field, prev_cursor}, query}, inclusive)
  end

  defp build_comparison_query({{:desc, field}, cursor}, query, true) do
    {{field, cursor}, or_where(query, [s], field(s, ^field) <= ^cursor)}
  end

  defp build_comparison_query({{:desc, field}, cursor}, query, _) do
    {{field, cursor}, or_where(query, [s], field(s, ^field) < ^cursor)}
  end

  defp build_comparison_query({{:asc, field}, cursor}, query, true) do
    {{field, cursor}, or_where(query, [s], field(s, ^field) >= ^cursor)}
  end

  defp build_comparison_query({{:asc, field}, cursor}, query, _) do
    {{field, cursor}, or_where(query, [s], field(s, ^field) > ^cursor)}
  end

  defp build_comparison_query({field, cursor}, query, inclusive) do
    build_comparison_query({{:asc, field}, cursor}, query, inclusive)
  end

  defp determine_start_after(repo, schema, {:asc, cursor}) do
    schema |> select([row], min(field(row, ^cursor))) |> repo.one
  end

  defp determine_start_after(repo, schema, {:desc, cursor}) do
    schema |> select([row], max(field(row, ^cursor))) |> repo.one
  end

  defp drop_inclusive(params) do
    Map.delete(params, :inclusive)
  end
end
