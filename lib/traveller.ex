defmodule Traveller do
  @moduledoc """
  Provides a simple way to fetch records from a database table.

  Supports cursor and offset based pagination. Allows for multiple cursor
  fields and multi-directional ordering.


  Simplest use-case:
  ```
  Traveller.start_stream(MyRepo, MySchema)
  ```

  Specify a cursor...
  ```
  Traveller.start_stream(MyRepo, MySchema, cursor: :first_name)
  ```

  ...or many
  ```
  Traveller.start_stream(MyRepo, MySchema, cursor: [asc: :first_name, desc: :last_name])
  ```

  Offset if you prefer:
  ```
  Traveller.start_stream(MyRepo, MySchema, mode: :offset)
  ```

  Start late and finish early:
  ```
  Traveller.start_stream(MyRepo, MySchema, start_after: "Albus", stop_before: "Severus")
  ```
  """

  import Ecto.Query

  @doc """
  Initiates a stream that walks through a database table.

  Expects a repo, a schema and 0 or more options.

  Assumes the repo passed has already been started.

  Options:
  - `cursor`: only applies when `mode = :cursor` (default). Defaults to `:id`. Can be an atom
  corresponding to a field in the schema passed; a tuple with an ordering
  (either `:asc` or `:desc`) and a field; or a list of fields or `{order, field}`
  tuples. If no order is specified `:asc` is assumed.

  - `chunk_size`: determines how many records are returned in each batch.

  - `start_after`: a value which is used for the initial cursor. Only
  works when `mode` is `cursor`. If none is provided then the highest or
  lowest value in the table is used, based on whether the ordering is descending
  or ascending respectively. If a list of cursor fields is passed, then
  the default applies to the first field only.

  - `stop_before`: a value which is used to terminate the record fetch early.
  Currently only supports a single value, and only works when `mode` is `cursor`.

  - `mode`: provide `:offset` if you want an offset based record fetch.

  - `order_by`: only applies when `mode` is `:offset`. Determines the order
  that records are returned in. Can be a field or a `{direction, field}`
  tuple. If no sort direction is specified, `:asc` is assumed.

  - `offset`: only applies when `mode` is `:offset`. This is used for the initial
  offset. The default is 0.

  - `next_cursor`: default is the values corresponding to the cursor fields
  from the last record the last record in the result set.
  You can provide a function which is passed the set of results
  which can return a value or a list of values used for the next cursor.
  """
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

        stop_before = Keyword.get(opts, :stop_before)

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
          cursor: cursor,
          inclusive: inclusive,
          next_cursor: next_cursor,
          start_after: start_after,
          stop_before: stop_before
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
    |> then(fn query ->
      if stop_before = params[:stop_before] do
        where(query, [s], field(s, ^cursor) < ^stop_before)
      else
        query
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
    |> then(fn query ->
      if stop_before = params[:stop_before] do
        where(query, [s], field(s, ^cursor) > ^stop_before)
      else
        query
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
