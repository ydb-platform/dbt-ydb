{{ config(primary_key='customer_id') }}

select
    id as customer_id,
    first_name,
    last_name

from {{ ref('raw_customers') }}
