{% macro ydb__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}
    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}
    {#-- opt-in only: this step never carried `sql_header`, so there is no fallback --#}
    {%- set sql_header = ydb_get_sql_header('merge_sql_header', fallback=none) -%}

{{ sql_header if sql_header is not none }}

-- update for matched (closing old versions)
UPDATE {{ target.render() }} ON
SELECT
    tmp.{{ columns.dbt_scd_id }} AS {{ columns.dbt_scd_id }},
    tmp.{{ columns.dbt_valid_to }} AS {{ columns.dbt_valid_to }}
FROM {{ target.render() }} snapshot
JOIN {{ source }} tmp ON tmp.{{ columns.dbt_scd_id }} = snapshot.{{ columns.dbt_scd_id }}
WHERE
    snapshot.{{ columns.dbt_valid_to }} IS NULL
    AND tmp.dbt_change_type IN ('update', 'delete');

-- upsert for new rows (not matched)
UPSERT INTO {{ target.render() }} ({{ insert_cols_csv }})
SELECT {{ insert_cols_csv }}
FROM {{ source }}
WHERE dbt_change_type = 'insert';
{%- endmacro %}
