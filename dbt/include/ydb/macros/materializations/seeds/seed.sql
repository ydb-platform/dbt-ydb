{% macro ydb__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {%- set primary_key_column = model['config'].get('primary_key', agate_table.column_names[0]) -%}

  {% set sql %}
    create table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {%- set is_primary_key = (column_name in primary_key_column.replace(' ', '').split(',')) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }}{% if is_primary_key %} NOT NULL{% endif %} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
        , primary key ({{ primary_key_column }})
    )
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}


{% macro ydb__load_csv_rows(model, agate_table) %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set data_sql = adapter.prepare_insert_values_from_csv(agate_table) %}

  {% if data_sql %}
    {% set sql -%}
      upsert into {{ this.render() }} ({{ cols_sql }}) VALUES
      {{ data_sql }}
    {%- endset %}

    {% do adapter.add_query(sql, bindings=agate_table, abridge_sql_log=True) %}
  {% endif %}
{% endmacro %}