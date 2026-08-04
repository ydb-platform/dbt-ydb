{#
    Per-statement SQL headers.

    `sql_header` is emitted in front of every statement a materialization runs.
    That is fine for session-level pragmas, but some YQL pragmas are bound to the
    physical plan of one particular statement -- `ydb.OverridePlanner` addresses
    stages by positional tx/stage ids -- so a single header can never be valid for
    all the statements of, say, the incremental materialization.

    Every statement that is not the "main" one therefore gets its own optional
    config key. When the key is set it *replaces* `sql_header` for that statement
    only; when it is not set the statement falls back to `sql_header`, which is the
    behaviour every model had before:

        sql_header        -- default for every statement
        merge_sql_header  -- the UPSERT of the incremental strategy
        tmp_sql_header    -- creation of the incremental temp relation

    Setting a per-statement key to an empty string emits no header at all for that
    statement, which is how a model opts out of the model-wide `sql_header`.
#}

{% macro ydb_get_sql_header(config_key=none, fallback='sql_header') -%}
  {%- set header = config.get(config_key, none) if config_key is not none else none -%}
  {%- if header is none and fallback is not none -%}
    {%- set header = config.get(fallback, none) -%}
  {%- endif -%}
  {{- return(header) -}}
{%- endmacro %}
