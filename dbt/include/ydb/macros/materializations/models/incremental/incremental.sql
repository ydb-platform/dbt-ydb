
{% materialization incremental, adapter='ydb' -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}

  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
      {% set relation_for_indexes = target_relation %}
  {% elif full_refresh_mode %}
      {% set build_sql = get_create_table_as_sql(False, intermediate_relation, sql) %}
      {% set relation_for_indexes = intermediate_relation %}
      {% set need_swap = true %}
  {% else %}
    {% do run_query(get_create_table_as_sql(False, temp_relation, sql)) %}
    {% do to_drop.append(temp_relation) %}
    {% set relation_for_indexes = temp_relation %}
    {% set contract_config = config.get('contract') %}
    {% if not contract_config or not contract_config.enforced %}
      {% do adapter.expand_target_column_types(
               from_relation=temp_relation,
               to_relation=target_relation) %}
    {% endif %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
    {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}

  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(relation_for_indexes) %}
  {% endif %}

  {% if need_swap %}
    {% if existing_relation.is_view %}
      {% call statement('main') -%}
        {{ get_create_view_as_sql(backup_relation, sql) }}
      {%- endcall %}
      {{ drop_relation(existing_relation) }}
    {% else %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
    {% endif %}
    {% do adapter.rename_relation(intermediate_relation, target_relation) %}
    {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro ydb__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set source_unique_key = ("DBT_INTERNAL_SOURCE." ~ unique_key) | trim %}
	    {% set target_unique_key = ("DBT_INTERNAL_DEST." ~ unique_key) | trim %}
	    {% set unique_key_match = equals(source_unique_key, target_unique_key) | trim %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    upsert into {{ target }}
    select {{ dest_cols_csv }} from {{ source }}

{% endmacro %}


{% macro ydb__get_incremental_default_sql(arg_dict) %}

  {% do return(get_incremental_merge_sql(arg_dict)) %}

{% endmacro %}