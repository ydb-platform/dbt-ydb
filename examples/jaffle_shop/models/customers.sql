{{ config(primary_key='customer_id') }}

select
    customers.customer_id as customer_id,
    customers.first_name as first_name,
    customers.last_name as last_name,
    customer_orders.first_order as first_order,
    customer_orders.most_recent_order as most_recent_order,
    customer_orders.number_of_orders as number_of_orders,
    customer_payments.total_amount as customer_lifetime_value

from {{ ref('stg_customers') }} as customers

left join (
    select
        customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from {{ ref('stg_orders') }}

    group by customer_id
) as customer_orders
    on customers.customer_id = customer_orders.customer_id

left join (
    select
    orders.customer_id as customer_id,
    sum(amount) as total_amount

    from {{ ref('stg_payments') }} as payments

    left join {{ ref('stg_orders') }} as orders on
            payments.order_id = orders.order_id

    group by orders.customer_id
) as customer_payments
    on  customers.customer_id = customer_payments.customer_id
