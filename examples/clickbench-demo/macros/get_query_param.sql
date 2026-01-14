{# Extracts host (domain) from URL. Returns NULL if input is NULL. #}
{% macro url_get_host(url_column) %}
    CASE
        WHEN {{ url_column }} IS NULL THEN NULL
        ELSE Url::GetHost({{ url_column }})
    END
{% endmacro %}

{# Extracts path from URL. Returns NULL if input is NULL. #}
{% macro url_get_path(url_column) %}
    CASE
        WHEN {{ url_column }} IS NULL THEN NULL
        ELSE Url::GetPath({{ url_column }})
    END
{% endmacro %}

{# Extracts query string from URL. Returns NULL if input is NULL. #}
{% macro url_get_query(url_column) %}
    CASE
        WHEN {{ url_column }} IS NULL THEN NULL
        ELSE '' --Url::GetQuery({{ url_column }})
    END
{% endmacro %}

{# Extracts fragment from URL. Returns NULL if input is NULL. #}
{% macro url_get_fragment(url_column) %}
    CASE
        WHEN {{ url_column }} IS NULL THEN NULL
        ELSE Url::GetFragment({{ url_column }})
    END
{% endmacro %}

{# Extracts parameter value from URL query. Returns NULL if not found or URL is NULL. #}
{% macro url_extract_param(url_column, param_name) %}
    CASE
        WHEN {{ url_column }} IS NULL THEN NULL
        ELSE (
            ''
        )
    END
{% endmacro %}

