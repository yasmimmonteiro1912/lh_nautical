-- =====================================================================
-- Model: agg_order_payments_summary
-- Layer: Gold / Marts
-- Description: One row per order_id, aggregating installment payments.
--              Prevents cartesian duplication when joined with fact
--              tables at order grain (e.g. fct_sales_items).
-- =====================================================================

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
    from payments
    group by
        order_id
        , customer_id
        , customer_sk
),

final_model as (
    select
        -- 1. Colunas simples primeiro
        order_id
        , customer_id
        , customer_sk
        , installment_count
        , total_paid_amount
        , first_payment_date
        , last_payment_date
        , distinct_payment_methods
        , installment_count > 1 as is_installment

        -- 2. Primary Surrogate Key
        , {{ dbt_utils.generate_surrogate_key(['order_id']) }}
            as order_payment_summary_sk

        -- 3. Timestamp de Auditoria
        , current_timestamp() as ingestion_timestamp
    from aggregated
)

select * from final_model