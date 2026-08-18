-- =====================================================================
-- Model: agg_order_payments_summary
-- Layer: Gold / Marts
-- Description: One row per order_id, aggregating installment payments
--              and comparing total paid vs total items sold (Dor II).
-- =====================================================================

with payments as (
    select * from {{ ref('fct_order_payments') }}
),

-- 1. Agrupamos o valor total dos itens vendidos por pedido
order_items_summary as (
    select
        order_id
        , sum(quantity * unit_price) as total_items_amount
    from {{ ref('int_orders_with_items') }}
    group by order_id
),

aggregated_payments as (
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
        -- Identificadores e Chaves
        pay.order_id
        , pay.customer_id
        , pay.customer_sk
        , {{ dbt_utils.generate_surrogate_key(['pay.order_id']) }}
            as order_payment_summary_sk

        -- Métricas de Pagamento
        , pay.installment_count
        , pay.total_paid_amount
        , coalesce(itm.total_items_amount, 0) as total_items_amount
        , pay.first_payment_date
        , pay.last_payment_date
        , pay.distinct_payment_methods
        , pay.installment_count > 1 as is_installment

        -- Flag da Dor II: Identifica divergência real entre o valor pago e o valor dos itens (> R$ 1,00)
        , case 
            when abs(pay.total_paid_amount - coalesce(itm.total_items_amount, 0)) > 1.00 then true 
            else false 
          end as is_payment_divergent

        -- Timestamp de Auditoria
        , current_timestamp() as ingestion_timestamp
    from aggregated_payments as pay
    left join order_items_summary as itm
        on pay.order_id = itm.order_id
)

select * from final_model