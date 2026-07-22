with payments as (

    select *
    from {{ ref('stg_lh_nautical_payments') }}

)

, orders as (

    select *
    from {{ ref('int_orders_with_items') }}

)

, customers as (

    select *
    from {{ ref('dim_customers') }}

)

, payment_events as (

    select
        payments.payment_id
        , payments.order_id
        , payments.payment_method
        , payments.payment_amount
        , payments.paid_at
        , orders.customer_id
    from payments
    left join orders
        on payments.order_id = orders.order_id
    where coalesce(orders.is_cancelled, false) = false

)

, final_fact as (

    select
        payment_events.payment_id as payment_sk
        , payment_events.order_id
        , coalesce(customers.customer_id, 'UNKNOWN') as customer_id
        , coalesce(date_format(payment_events.paid_at, 'yyyyMMdd'), 'UNKNOWN') as date_sk
        , lower(trim(payment_events.payment_method)) as payment_method
        , cast(payment_events.payment_amount as decimal(14, 2)) as payment_amount

    from payment_events
    left join customers
        on payment_events.customer_id = customers.customer_id

)

select *
from final_fact