{% macro ydb__generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {%- if default_schema == '' -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema }}/{{ custom_schema_name | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}