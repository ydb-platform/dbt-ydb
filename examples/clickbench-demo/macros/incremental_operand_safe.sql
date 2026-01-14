-- Generates a safe incremental filter for YDB.
-- Usage: {{ incremental_filter('event_time') }}
-- Returns: (no output if not incremental) or a Jinja block that sets a filter

{% macro incremental_operand_safe(timestamp_field) %}

    {%- set default_ts = '1970-01-01T00:00:00Z' -%}
    {%- set cutoff = default_ts -%}

    {%- if is_incremental() and execute -%}

        {%- set query %}
            SELECT 
                COALESCE(
                    CAST(MAX({{ timestamp_field }}) AS Utf8),
                    '{{ default_ts }}'
                )
            FROM {{ this }}
        {%- endset %}

        {%- set result = run_query(query) -%}
        {%- if result and result.rows -%}
            {%- set cutoff = result.rows[0][0] -%}
        {%- endif -%}

    {%- endif -%}

    CAST('{{ cutoff }}' AS Timestamp)

{% endmacro %}


