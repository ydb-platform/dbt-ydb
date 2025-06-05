{{ config(primary_key='payment_id') }}

select
    id as payment_id,
    order_id,
    payment_method,

    -- `amount` is currently stored in cents, so we convert it to dollars
    amount / 100 as amount

from {{ ref('raw_payments') }}
