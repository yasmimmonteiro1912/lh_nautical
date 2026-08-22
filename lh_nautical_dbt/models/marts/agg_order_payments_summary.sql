with payments as (
    select * from {{ ref('fct_order_payments') }}
),

aggregated as (
    select
        order_id
        , customer_id
        , customer_sk
        , count(payment_id) as installment_count
        , sum(payment_amount) as total_paid_amount
        , min(paid_at) as first_payment_date
        , max(paid_at) as last_payment_date
        , count(distinct payment_method) as distinct_payment_methods
        , string_agg(distinct payment_method, ', ') as payment_methods_used
    from payments
    group by
        order_id
        , customer_id
        , customer_sk
),

primary_method as (
    select
        order_id
        , payment_method as primary_payment_method
    from payments
    qualify row_number() over (
        partition by order_id
        order by paid_at asc
    ) = 1
),

order_items_summary as (
    select
        order_id
        , sum(quantity * unit_price) as total_items_amount
    from {{ ref('int_orders_with_items') }}
    group by order_id
),

final_model as (
    select
        -- 1. Colunas simples primeiro
        pay.order_id
        , pay.customer_id
        , pay.customer_sk
        , pay.installment_count
        , pay.total_paid_amount
        , pay.first_payment_date
        , pay.last_payment_date
        , pay.distinct_payment_methods
        , pay.payment_methods_used
        , pm.primary_payment_method
        , pay.installment_count > 1 as is_installment

        -- 2. Primary Surrogate Key
        , {{ dbt_utils.generate_surrogate_key(['pay.order_id']) }}
            as order_payment_summary_sk

        , coalesce(itm.total_items_amount, 0) as total_items_amount
        , case
            when abs(pay.total_paid_amount - coalesce(itm.total_items_amount, 0)) > 1.00 then true
            else false
        end as is_payment_divergent

        -- 3. Timestamp de Auditoria
        , current_timestamp() as ingestion_timestamp
    from aggregated as pay
    left join primary_method as pm
        on pay.order_id = pm.order_id
    left join order_items_summary as itm
        on pay.order_id = itm.order_id
)

select * from final_model