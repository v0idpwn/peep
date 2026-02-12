defmodule Peep.Storage.ETS do
  @moduledoc """
  Peep.Storage implementation using two ETS tables.

  Counters, sums, and last values are stored in a main table optimized for
  concurrent writes (`write_concurrency: true`).

  Distributions are stored in a separate table with `read_concurrency: true`,
  since after the initial insert of an atomics struct, subsequent operations
  are predominantly reads (lookups) followed by lock-free atomics updates.
  """
  alias Peep.Storage
  alias Telemetry.Metrics

  @behaviour Peep.Storage

  @impl true
  def new(_) do
    main =
      :ets.new(__MODULE__, [
        :public,
        read_concurrency: false,
        write_concurrency: true,
        decentralized_counters: true
      ])

    dist =
      :ets.new(:peep_distributions, [
        :public,
        read_concurrency: true,
        write_concurrency: false
      ])

    {main, dist}
  end

  @impl true
  def storage_size({main, dist}) do
    %{
      size: :ets.info(main, :size) + :ets.info(dist, :size),
      memory:
        (:ets.info(main, :memory) + :ets.info(dist, :memory)) * :erlang.system_info(:wordsize)
    }
  end

  @impl true
  def insert_metric({main, _dist}, id, %Metrics.Counter{}, _value, %{} = tags) do
    key = {id, tags, :erlang.system_info(:scheduler_id)}
    :ets.update_counter(main, key, {2, 1}, {key, 0})
  end

  def insert_metric({main, _dist}, id, %Metrics.Sum{}, value, %{} = tags) do
    key = {id, tags, :erlang.system_info(:scheduler_id)}
    :ets.update_counter(main, key, {2, value}, {key, 0})
  end

  def insert_metric({main, _dist}, id, %Metrics.LastValue{}, value, %{} = tags) do
    key = {id, tags}
    :ets.insert(main, {key, value})
  end

  def insert_metric({_main, dist}, id, %Metrics.Distribution{} = metric, value, %{} = tags) do
    key = {id, tags}

    atomics =
      case :ets.lookup(dist, key) do
        [{_key, ref}] ->
          ref

        [] ->
          # Race condition: Multiple processes could be attempting
          # to write to this key. Thankfully, :ets.insert_new/2 will break ties,
          # and concurrent writers should agree on which :atomics object to
          # increment.
          new_atomics = Storage.Atomics.new(metric)

          case :ets.insert_new(dist, {key, new_atomics}) do
            true ->
              new_atomics

            false ->
              [{_key, atomics}] = :ets.lookup(dist, key)
              atomics
          end
      end

    Storage.Atomics.insert(atomics, value)
  end

  @impl true
  def get_all_metrics({main, dist}, %Peep.Persistent{ids_to_metrics: itm}) do
    :ets.tab2list(main)
    |> group_metrics(itm, %{})
    |> then(fn acc ->
      :ets.tab2list(dist)
      |> group_metrics(itm, acc)
    end)
  end

  @impl true
  def get_metric({main, _dist}, id, %Metrics.Counter{}, tags) do
    :ets.select(main, [{{{id, :"$2", :_}, :"$1"}, [{:==, :"$2", tags}], [:"$1"]}])
    |> Enum.reduce(0, fn count, acc -> count + acc end)
  end

  def get_metric({main, _dist}, id, %Metrics.Sum{}, tags) do
    :ets.select(main, [{{{id, :"$2", :_}, :"$1"}, [{:==, :"$2", tags}], [:"$1"]}])
    |> Enum.reduce(0, fn count, acc -> count + acc end)
  end

  def get_metric({main, _dist}, id, %Metrics.LastValue{}, tags) do
    case :ets.lookup(main, {id, tags}) do
      [{_key, value}] -> value
      _ -> nil
    end
  end

  def get_metric({_main, dist}, id, %Metrics.Distribution{}, tags) do
    key = {id, tags}

    case :ets.lookup(dist, key) do
      [{_key, atomics}] -> Storage.Atomics.values(atomics)
      _ -> nil
    end
  end

  @impl true
  def prune_tags({main, dist}, patterns) do
    main_match_spec =
      patterns
      |> Enum.flat_map(fn pattern ->
        counter_or_sum_key = {:_, pattern, :_}
        last_value_key = {:_, pattern}

        [
          {{counter_or_sum_key, :_}, [], [true]},
          {{last_value_key, :_}, [], [true]}
        ]
      end)

    dist_match_spec =
      patterns
      |> Enum.flat_map(fn pattern ->
        [{{{:_, pattern}, :_}, [], [true]}]
      end)

    :ets.select_delete(main, main_match_spec)
    :ets.select_delete(dist, dist_match_spec)
    :ok
  end

  defp group_metrics([], _itm, acc) do
    acc
  end

  defp group_metrics([metric | rest], itm, acc) do
    acc2 = group_metric(metric, itm, acc)
    group_metrics(rest, itm, acc2)
  end

  defp group_metric({{id, tags, _}, value}, itm, acc) do
    %{^id => metric} = itm
    update_in(acc, [Access.key(metric, %{}), Access.key(tags, 0)], &(&1 + value))
  end

  defp group_metric({{id, tags}, %Storage.Atomics{} = atomics}, itm, acc) do
    %{^id => metric} = itm
    put_in(acc, [Access.key(metric, %{}), Access.key(tags)], Storage.Atomics.values(atomics))
  end

  defp group_metric({{id, tags}, value}, itm, acc) do
    %{^id => metric} = itm
    put_in(acc, [Access.key(metric, %{}), Access.key(tags)], value)
  end
end
