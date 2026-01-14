-- macros/types.sql
{% macro type_Int64() %} Int64 {% endmacro %}
{% macro type_Int32() %} Int32 {% endmacro %}
{% macro type_Int16() %} Int16 {% endmacro %}
{% macro type_Utf8() %} Utf8 {% endmacro %}
{% macro type_String() %} String {% endmacro %}
{% macro type_Timestamp() %} Timestamp {% endmacro %}
{% macro type_Date() %} Date {% endmacro %}
{% macro type_Bool() %} Bool {% endmacro %}

{% macro to_bool(column) %}
    CASE {{ column }}
        WHEN 1 THEN TRUE 
        WHEN 0 THEN FALSE 
        ELSE NULL 
    END
{% endmacro %}

-- macro: to_int8.sql
-- Converts a boolean expression to Int8 (1 = TRUE, 0 = FALSE, NULL = NULL)
-- Useful for YDB columnar tables that do not support Bool type.

{% macro to_int8(column) %}
    CAST(
        CASE {{ column }}
            WHEN TRUE THEN 1
            WHEN FALSE THEN 0
            ELSE NULL 
        END AS Int8
    )
{% endmacro %}