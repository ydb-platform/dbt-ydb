{#--
    The `microbatch` incremental strategy.

    dbt cuts the model into batches along its `event_time` column and runs the
    materialization once per batch, each time with the batch's `[start, end)` window in
    `model.batch` and the model query already filtered down to that window.

    The window is what a batch owns, so it is cleared in the target first and then
    rewritten from the staged rows. That is what makes a batch re-runnable: rows that
    left the window since the last run disappear from the target instead of lingering
    there, and rows that arrived late land next to the ones already written.

    YQL has neither MERGE nor an alias in DELETE, so the window is spelled out as a
    predicate over the target's own columns. Both statements go out as one script, which
    YDB runs as a single transaction.
--#}

{% macro ydb__get_incremental_microbatch_sql(arg_dict) %}
  {%- set target = arg_dict["target_relation"] -%}
  {%- set source = arg_dict["temp_relation"] -%}
  {%- set dest_cols_csv = get_quoted_csv(arg_dict["dest_columns"] | map(attribute="name")) -%}

  {%- set predicates = ydb_microbatch_predicates(arg_dict.get("incremental_predicates")) -%}

  {#-- this statement has a plan of its own, so it takes `merge_sql_header` when the
       model sets one and falls back to `sql_header` otherwise --#}
  {%- set sql_header = ydb_get_sql_header('merge_sql_header') -%}

{% if sql_header is not none %}
{{ sql_header }}

{% endif %}
    delete from {{ target }}
    where {{ predicates | join('\n      and ') }};

    upsert into {{ target }}
    select
        {{ dest_cols_csv }}
    from {{ source }}

{% endmacro %}


{#--
    The predicate that selects the rows of the current batch in the target: the batch
    window over `event_time`, plus whatever `incremental_predicates` the model adds.

    DELETE takes no alias in YQL, so an `incremental_predicates` entry has to be written
    against the target's bare column names -- there is no `DBT_INTERNAL_DEST` to qualify
    them with.
--#}
{% macro ydb_microbatch_predicates(incremental_predicates=none) %}
  {%- set event_time = model['config'].get('event_time') -%}

  {%- if not event_time -%}
    {{ exceptions.raise_compiler_error(
        "The `microbatch` incremental strategy requires an `event_time` config on model '" ~ model.name ~ "'") }}
  {%- endif -%}

  {#-- `model.batch` is where dbt puts the window; the config keys are the pre-1.9 spelling --#}
  {%- set batch = model.get('batch') or {} -%}
  {%- set batch_start = batch.get('event_time_start') or model['config'].get('__dbt_internal_microbatch_event_time_start') -%}
  {%- set batch_end = batch.get('event_time_end') or model['config'].get('__dbt_internal_microbatch_event_time_end') -%}

  {%- if not batch_start and not batch_end -%}
    {#-- no window means every row of the target would match, so refuse rather than wipe it --#}
    {{ exceptions.raise_compiler_error(
        "Model '" ~ model.name ~ "' uses the `microbatch` strategy, but dbt did not pass a batch window. "
        ~ "The strategy needs dbt's batched execution, which dbt-core runs microbatch models with from 1.9 on.") }}
  {%- endif -%}

  {%- set predicates = [] -%}
  {%- if batch_start -%}
    {%- do predicates.append(event_time ~ ' >= ' ~ adapter.timestamp_literal(batch_start)) -%}
  {%- endif -%}
  {%- if batch_end -%}
    {%- do predicates.append(event_time ~ ' < ' ~ adapter.timestamp_literal(batch_end)) -%}
  {%- endif -%}
  {%- do predicates.extend(incremental_predicates or []) -%}

  {{ return(predicates) }}
{% endmacro %}
