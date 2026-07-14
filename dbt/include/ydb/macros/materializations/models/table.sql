{% macro ydb__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {%- set primary_key_expr = model['config'].get('primary_key') -%}
  {%- if not primary_key_expr -%}
    {{ exceptions.raise_compiler_error("Configuration parameter `primary_key` should be specified for model '" + model.name + "'") }}
  {%- endif -%}

  {%- set store_type = model['config'].get('store_type', 'row') -%}

  {%- set table_options = ['STORE = ' ~ store_type] -%}

  {%- set with_settings = [
      ('auto_partitioning_by_size', 'AUTO_PARTITIONING_BY_SIZE'),
      ('auto_partitioning_by_load', 'AUTO_PARTITIONING_BY_LOAD'),
      ('auto_partitioning_partition_size_mb', 'AUTO_PARTITIONING_PARTITION_SIZE_MB'),
      ('auto_partitioning_min_partitions_count', 'AUTO_PARTITIONING_MIN_PARTITIONS_COUNT'),
      ('auto_partitioning_max_partitions_count', 'AUTO_PARTITIONING_MAX_PARTITIONS_COUNT'),
      ('uniform_partitions', 'UNIFORM_PARTITIONS'),
      ('partition_at_keys', 'PARTITION_AT_KEYS'),
      ('ttl', 'TTL'),
  ] -%}
  {%- for cfg_key, sql_key in with_settings -%}
    {%- set value = model['config'].get(cfg_key) -%}
    {%- if value is not none -%}
      {%- do table_options.append(sql_key ~ ' = ' ~ value) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set partition_by = model['config'].get('partition_by') -%}
  {%- if partition_by is not none and partition_by is not string and partition_by is sequence -%}
    {%- set partition_by = partition_by | join(', ') -%}
  {%- endif -%}
  {%- set partition_method = model['config'].get('partition_method', 'hash') -%}
  {%- if partition_by is not none and store_type != 'column' -%}
    {{ exceptions.raise_compiler_error("Configuration parameter `partition_by` requires `store_type='column'` for model '" + model.name + "'") }}
  {%- endif -%}

  {{ sql_header if sql_header is not none }}

create {% if temporary %}temporary {% endif %}table
  {{ relation.include(database=(not temporary), schema=(not temporary)) }}
(
  primary key ({{ primary_key_expr }})
)
{%- set contract_config = config.get('contract') %}
{% if contract_config.enforced and (not temporary) %}
{{ get_assert_columns_equivalent(sql) }}
{{ get_table_columns_and_constraints() }}
  {%- set sql = get_select_subquery(sql) %}
{% endif %}
{% if partition_by is not none %}PARTITION BY {{ partition_method | upper }} ({{ partition_by }})
{% endif %}WITH (
  {{ table_options | join(',\n  ') }}
)
as
{{ sql }}

{%- endmacro %}
