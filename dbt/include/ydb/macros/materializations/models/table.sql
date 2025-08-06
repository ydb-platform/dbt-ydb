{% macro ydb__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {%- set primary_key_expr = model['config'].get('primary_key') -%}
  {%- if not primary_key_expr -%}
    {{ exceptions.raise_compiler_error("Configuration parameter `primary_key` should be specified for model '" + model.name + "'") }}
  {%- endif -%}

  {%- set store_type = model['config'].get('store_type', 'row') -%}

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
  as (
  {{ sql }}
  )
{%- endmacro %}