
{% macro ydb__build_snapshot_table(strategy, sql) %}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}

    select `sbq`.*,
        {{ strategy.scd_id }} as {{ columns.dbt_scd_id }},
        {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
        {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
        {{ ydb__get_dbt_valid_to_current(strategy, columns) }}
      {%- if strategy.hard_deletes == 'new_record' -%}
        , 'False' as {{ columns.dbt_is_deleted }}
      {% endif -%}
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% macro ydb__get_dbt_valid_to_current(strategy, columns) %}
  {% set dbt_valid_to_current = config.get('dbt_valid_to_current') or "null" %}
  coalesce(case when {{ strategy.updated_at }} = {{ strategy.updated_at }} then null else {{ strategy.updated_at }} end, {{dbt_valid_to_current}})
  as {{ columns.dbt_valid_to }}
{% endmacro %}

{% macro ydb__build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}

    {% do drop_relation(temp_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ ydb__create_snapshot_table_as(False, temp_relation, select) }}
    {% endcall %}

    {% do return(temp_relation) %}
{% endmacro %}

{% macro ydb__create_snapshot_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}

  {%- set primary_key_expr = config.get('primary_key', columns.dbt_scd_id) -%}

  {%- set store_type = config.get('store_type', 'row') -%}

  {%- set auto_partitioning_by_size = model['config'].get('auto_partitioning_by_size') -%}
  {%- set auto_partitioning_partition_size_mb = model['config'].get('auto_partitioning_partition_size_mb') -%}
  {%- set ttl_expr = model['config'].get('ttl') -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  (primary key ({{ primary_key_expr }}))
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced and (not temporary) %}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {%- set sql = get_select_subquery(sql) %}
  {% endif %}
  WITH (
    STORE = {{ store_type }}
    {%- if auto_partitioning_by_size is not none -%}
    , AUTO_PARTITIONING_BY_SIZE = {{ auto_partitioning_by_size }}
    {%- endif -%}
    {%- if auto_partitioning_partition_size_mb is not none -%}
    , AUTO_PARTITIONING_PARTITION_SIZE_MB = {{ auto_partitioning_partition_size_mb }}
    {%- endif -%}
    {%- if ttl_expr is not none -%}
    , TTL = {{ ttl_expr }}
    {%- endif -%}
  )
  as
  {{ sql }}

{%- endmacro %}


{% macro ydb__snapshot_staging_table(strategy, source_sql, target_relation) -%}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}
    {% if strategy.hard_deletes == 'new_record' %}
        {% set new_scd_id = snapshot_hash_arguments([columns.dbt_scd_id, snapshot_get_time()]) %}
    {% endif %}

    select `insertions`.* from (
        select
            'insert' as dbt_change_type,
            `source_data`.*
          {%- if strategy.hard_deletes == 'new_record' -%}
            ,'False' as {{ columns.dbt_is_deleted }}
          {%- endif %}

        from (
            select `source`.*, {{ unique_key_fields(strategy.unique_key) }},
                {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
                {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
                {{ ydb__get_dbt_valid_to_current(strategy, columns) }},
                {{ strategy.scd_id }} as {{ columns.dbt_scd_id }}

            from ({{ source_sql }}) as source

        ) as source_data
        left outer join (
            select `target`.*, {{ unique_key_fields(strategy.unique_key) }}
            from {{ target_relation }} as target
            where
                {% if config.get('dbt_valid_to_current') %}
            {% set source_unique_key = columns.dbt_valid_to | trim %}
            {% set target_unique_key = config.get('dbt_valid_to_current') | trim %}

            {# The exact equals semantics between NULL values depends on the current behavior flag set. Also, update records if the source field is null #}
                    ( {{ equals(source_unique_key, target_unique_key) }} or {{ source_unique_key }} is null )
                {% else %}
                    {{ columns.dbt_valid_to }} is null
                {% endif %}
        ) as snapshotted_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
            where {{ unique_key_is_null(strategy.unique_key, "snapshotted_data") }}
            or ({{ unique_key_is_not_null(strategy.unique_key, "snapshotted_data") }} and (
               {{ strategy.row_changed }} {%- if strategy.hard_deletes == 'new_record' -%} or snapshotted_data.{{ columns.dbt_is_deleted }} = 'True' {% endif %}
            )

        )
    ) as insertions
    union all
    select `updates`.* from (
        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.{{ columns.dbt_scd_id }} as {{ columns.dbt_scd_id }},
          {%- if strategy.hard_deletes == 'new_record' -%}
            , snapshotted_data.{{ columns.dbt_is_deleted }}
          {%- endif %}

        from (
            select `source`.*, {{ unique_key_fields(strategy.unique_key) }},
                {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
                {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
                {{ strategy.updated_at }} as {{ columns.dbt_valid_to }}

            from ({{ source_sql }}) as source
        ) as source_data
        join (
            select `target`.*, {{ unique_key_fields(strategy.unique_key) }}
            from {{ target_relation }} as target
            where
                {% if config.get('dbt_valid_to_current') %}
            {% set source_unique_key = columns.dbt_valid_to | trim %}
            {% set target_unique_key = config.get('dbt_valid_to_current') | trim %}

            {# The exact equals semantics between NULL values depends on the current behavior flag set. Also, update records if the source field is null #}
                    ( {{ equals(source_unique_key, target_unique_key) }} or {{ source_unique_key }} is null )
                {% else %}
                    {{ columns.dbt_valid_to }} is null
                {% endif %}
        ) as snapshotted_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
        where (
            {{ strategy.row_changed }}  {%- if strategy.hard_deletes == 'new_record' -%} or snapshotted_data.{{ columns.dbt_is_deleted }} = 'True' {% endif %}
        )
    ) as updates
    {%- if strategy.hard_deletes == 'invalidate' or strategy.hard_deletes == 'new_record' %}
    union all
    select `deletes`.* from (
        select
            'delete' as dbt_change_type,
            -- source_data.*,  we are not able to use it because we lose not null constraint on unique key
            snapshotted_data.{{ strategy.unique_key }} as {{ strategy.unique_key }},

            {{ snapshot_get_time() }} as {{ columns.dbt_valid_from }},
            {{ snapshot_get_time() }} as {{ columns.dbt_updated_at }},
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_to }},
            snapshotted_data.{{ columns.dbt_scd_id }} as {{ columns.dbt_scd_id }},
          {%- if strategy.hard_deletes == 'new_record' -%}
            , snapshotted_data.{{ columns.dbt_is_deleted }}
          {%- endif %}
        from (
            select `target`.*, {{ unique_key_fields(strategy.unique_key) }}
            from {{ target_relation }} as target
            where
                {% if config.get('dbt_valid_to_current') %}
            {% set source_unique_key = columns.dbt_valid_to | trim %}
            {% set target_unique_key = config.get('dbt_valid_to_current') | trim %}

            {# The exact equals semantics between NULL values depends on the current behavior flag set. Also, update records if the source field is null #}
                    ( {{ equals(source_unique_key, target_unique_key) }} or {{ source_unique_key }} is null )
                {% else %}
                    {{ columns.dbt_valid_to }} is null
                {% endif %}
        ) as snapshotted_data
        left join (
            select `source`.*, {{ unique_key_fields(strategy.unique_key) }}
            from ({{ source_sql }}) as source
        ) as source_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
            where {{ unique_key_is_null(strategy.unique_key, "source_data") }}

            {%- if strategy.hard_deletes == 'new_record' %}
            and not (
                --avoid updating the record's valid_to if the latest entry is marked as deleted
                snapshotted_data.{{ columns.dbt_is_deleted }} = 'True'
                and snapshotted_data.{{ columns.dbt_valid_to }} is null
            )
            {%- endif %}
    ) as deletes
    {%- endif %}
    {%- if strategy.hard_deletes == 'new_record' %}
    union all
    select `deletion_records`.* from (
        select
            'insert' as dbt_change_type,
            {#
                If a column has been added to the source it won't yet exist in the
                snapshotted table so we insert a null value as a placeholder for the column.
             #}
            {%- for col in source_sql_cols -%}
            {%- if col.name in snapshotted_cols -%}
            snapshotted_data.{{ adapter.quote(col.column) }},
            {%- else -%}
            NULL as {{ adapter.quote(col.column) }},
            {%- endif -%}
            {% endfor -%}
            {%- if strategy.unique_key | is_list -%}
                {%- for key in strategy.unique_key -%}
            snapshotted_data.{{ key }} as dbt_unique_key_{{ loop.index }},
                {% endfor -%}
            {%- else -%}
            snapshotted_data.dbt_unique_key as dbt_unique_key,
            {% endif -%}
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_from }},
            {{ snapshot_get_time() }} as {{ columns.dbt_updated_at }},
            snapshotted_data.{{ columns.dbt_valid_to }} as {{ columns.dbt_valid_to }},
            {{ new_scd_id }} as {{ columns.dbt_scd_id }},
            'True' as {{ columns.dbt_is_deleted }}
        from (
            select `target`.*, {{ unique_key_fields(strategy.unique_key) }}
            from {{ target_relation }} as target
            where
                {% if config.get('dbt_valid_to_current') %}
            {% set source_unique_key = columns.dbt_valid_to | trim %}
            {% set target_unique_key = config.get('dbt_valid_to_current') | trim %}

            {# The exact equals semantics between NULL values depends on the current behavior flag set. Also, update records if the source field is null #}
                    ( {{ equals(source_unique_key, target_unique_key) }} or {{ source_unique_key }} is null )
                {% else %}
                    {{ columns.dbt_valid_to }} is null
                {% endif %}
        ) as snapshotted_data
        left join (
            select `source`.*, {{ unique_key_fields(strategy.unique_key) }}
            from ({{ source_sql }}) as source
        ) as source_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
        where {{ unique_key_is_null(strategy.unique_key, "source_data") }}
        and not (
            --avoid inserting a new record if the latest one is marked as deleted
            snapshotted_data.{{ columns.dbt_is_deleted }} = 'True'
            and snapshotted_data.{{ columns.dbt_valid_to }} is null
            )
    ) as deletion_records
    {%- endif %}


{%- endmacro %}