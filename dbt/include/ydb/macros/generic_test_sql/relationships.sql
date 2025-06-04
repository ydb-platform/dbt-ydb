{% macro ydb__test_relationships(model, column_name, to, field) %}

select
    child.from_field
from (
    select {{ column_name }} as from_field
    from {{ model }}
    where {{ column_name }} is not null
) as child
left join (
    select {{ field }} as to_field
    from {{ to }}
) as parent
on child.from_field = parent.to_field
where parent.to_field is null

{% endmacro %}
