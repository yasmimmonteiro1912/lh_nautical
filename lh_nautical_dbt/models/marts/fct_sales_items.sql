with order_items as (
    select * from {{ ref('stg_lh_nautical_order_items') }}
),

orders as (
    select * from {{ ref('stg_lh_nautical_orders') }}
),

fiscal_invoices as (
    select * from {{ ref('int_active_fiscal_invoices') }}
),

joined_data as (
    select
        -- Chaves naturais e dimensões degeneradas
        itm.order_item_id
        , ord.order_id
        , ord.order_number
        , ord.customer_id
        , itm.product_variant_id
        , ord.location_id
        , ord.salesperson_id as employee_id
        , cast(ord.placed_at as date) as order_date
        , ord.channel

        -- Métricas brutas da linha de item
        , itm.quantity
        , itm.unit_price
        , itm.line_total

        -- Dados fiscais da nota ativa (Nome correto da coluna da intermediate)
        , fsc.nfe_number
        , fsc.nfe_series
        , fsc.invoice_status
        , fsc.active_total_amount as invoice_total_amount

        -- Timestamp de auditoria
        , itm.ingestion_timestamp
    from order_items as itm
    left join orders as ord
        on itm.order_id = ord.order_id
    left join fiscal_invoices as fsc
        on ord.order_id = fsc.order_id
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
        , unit_price
        , line_total
        , nfe_number
        , nfe_series
        , invoice_status
        , invoice_total_amount
        , ingestion_timestamp

        -- 2. Cálculos de Receita e Desconto na linha
        , (quantity * unit_price) as gross_revenue
        , line_total as net_revenue
        , coalesce((quantity * unit_price) - line_total, 0) as discount_amount

        -- 3. Geração das Surrogate Keys (SKs) para o Star Schema
        , {{ dbt_utils.generate_surrogate_key(['order_item_id']) }} as sales_item_sk
        , {{ dbt_utils.generate_surrogate_key(['order_date']) }} as date_sk
        , {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk
        , {{ dbt_utils.generate_surrogate_key(['product_variant_id']) }} as variant_sk
        , {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_sk
        , {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_sk
    from joined_data
)

-- 4. SELECT final estruturado para a camada Gold
select
    sales_item_sk
    , date_sk
    , customer_sk
    , variant_sk
    , location_sk
    , employee_sk
    , order_id
    , order_number
    , channel
    , quantity
    , unit_price
    , discount_amount
    , gross_revenue
    , net_revenue
    , nfe_number
    , nfe_series
    , invoice_status
    , invoice_total_amount
    , ingestion_timestamp
from calculated_metrics_and_sks