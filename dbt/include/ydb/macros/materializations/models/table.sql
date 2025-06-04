{% macro ydb__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set primary_key_column = model['config'].get('primary_key', 'id') -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  (primary key ({{ primary_key_column }}))
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced and (not temporary) %}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {%- set sql = get_select_subquery(sql) %}
  {% endif %}
  as (
  {{ sql }}
  )
{%- endmacro %}