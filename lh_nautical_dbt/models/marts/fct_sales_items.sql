-- =====================================================================
-- Model: fct_sales_items
-- Description: Fact table recording item-level sales, line discounts,
--              active fiscal invoices, and net revenue after returns.
-- =====================================================================

with order_items_enriched as (
    select * from {{ ref('int_orders_with_items') }}
),

fiscal_invoices as (
    select * from {{ ref('int_active_fiscal_invoices') }}
),

returns_summary as (
    select
        order_id
        , sum(total_returned_quantity) as returned_quantity
        , sum(total_refund_amount) as refund_amount
    from {{ ref('int_returns') }}
    group by order_id
),

joined_data as (
    select
        -- Chaves naturais e dimensões degeneradas
        itm.order_item_id
        , itm.order_id
        , itm.order_number
        , itm.customer_id
        , itm.product_variant_id
        , itm.location_id
        , itm.employee_id
        , cast(itm.placed_at as date) as order_date
        , itm.order_channel as channel

        -- Métricas brutas da linha de item
        , itm.quantity
        , itm.unit_price
        , itm.line_total

        -- Métricas do pedido, para rateio do desconto
        , itm.order_subtotal
        , itm.order_discount_amount

        -- Métricas de Devolução (Consumidas da int_returns_with_items)
        , coalesce(retn.returned_quantity, 0) as returned_quantity
        , coalesce(retn.refund_amount, 0) as refund_amount

        -- Dados fiscais da nota ativa
        , fsc.nfe_number
        , fsc.nfe_series
        , fsc.invoice_status
        , fsc.active_total_amount as invoice_total_amount

        -- Timestamp de auditoria
        , itm.ingestion_timestamp
    from order_items_enriched as itm
    left join returns_summary as retn
        on itm.order_id = retn.order_id
    left join fiscal_invoices as fsc
        on itm.order_id = fsc.order_id
),

calculated_metrics_and_sks as (
    select
        -- 1. Colunas puras primeiro (Regra ST06)
        order_item_id
        , order_id
        , order_number
        , customer_id
        , product_variant_id
        , location_id
        , employee_id
        , order_date
        , channel
        , quantity
        , returned_quantity
        , unit_price
        , line_total
        , refund_amount
        , nfe_number
        , nfe_series
        , invoice_status
        , invoice_total_amount
        , ingestion_timestamp

        -- 2. Cálculos de Receita, Desconto e Métricas Líquidas
        , (quantity * unit_price) as gross_revenue
        , line_total as gross_sales_amount
        , coalesce(
            (line_total / nullif(order_subtotal, 0)) * order_discount_amount
        , 0) as discount_amount
        , (quantity - returned_quantity) as net_quantity
        , (line_total - refund_amount) as net_sales_amount

        -- 3. Geração das Surrogate Keys (SKs) para o Star Schema
        , {{ dbt_utils.generate_surrogate_key([
            'order_item_id'
        ]) }} as sales_item_sk

        , cast(date_format(order_date, 'yyyyMMdd') as int) as date_sk

        , {{ dbt_utils.generate_surrogate_key([
            'customer_id'
        ]) }} as customer_sk

        , {{ dbt_utils.generate_surrogate_key([
            'product_variant_id'
        ]) }} as variant_sk

        , {{ dbt_utils.generate_surrogate_key([
            'location_id'
        ]) }} as location_sk

        , case
            when employee_id is not null
                then {{ dbt_utils.generate_surrogate_key([
                    'employee_id'
                ]) }}
        end as employee_sk
    from joined_data
)

-- 4. SELECT final estruturado para a camada Gold
select
    -- Primary Key & Foreign Keys
    sales_item_sk
    , date_sk
    , customer_sk
    , variant_sk
    , location_sk
    , employee_sk

    -- Natural Keys / Degenerate Dimensions
    , order_item_id
    , order_id
    , order_number
    , customer_id
    , product_variant_id
    , location_id
    , employee_id
    , channel

    -- Quantidades e Valores Brutos
    , quantity
    , unit_price
    , gross_revenue
    , discount_amount
    , gross_sales_amount

    -- Impacto de Devoluções
    , returned_quantity
    , refund_amount

    -- Métricas Líquidas de Venda
    , net_quantity
    , net_sales_amount

    -- Fiscal
    , nfe_number
    , nfe_series
    , invoice_status
    , invoice_total_amount

    -- Metadata
    , ingestion_timestamp
from calculated_metrics_and_sks