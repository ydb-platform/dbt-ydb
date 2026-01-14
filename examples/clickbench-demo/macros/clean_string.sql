{% macro clean_string(column) %}
    IF(
        String::Strip(CAST({{ column }} AS Utf8)) = '',
        NULL,
        String::Strip(CAST({{ column }} AS Utf8))
    )
{% endmacro %}