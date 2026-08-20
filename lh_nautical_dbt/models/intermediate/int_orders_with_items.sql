-- =====================================================================
-- Model: int_orders_items
-- Description: Intermediate model combining sales order headers with line items
--              at the order_item_id granularity without aggregations.
-- =====================================================================

with orders as (
    select
        order_id
        , customer_id
        , location_id
        , salesperson_id as employee_id
        , order_number
        , lower(trim(status)) as order_status
        , lower(trim(channel)) as order_channel
        , subtotal as order_subtotal
        , discount_amount as order_discount_amount
        , placed_at
        , created_at
        , updated_at
    from {{ ref('stg_lh_nautical_orders') }}
),

order_items as (
    select
        order_item_id
        , order_id
        , product_variant_id
        , quantity
        , unit_price
        , line_total
        , ingestion_timestamp
    from {{ ref('stg_lh_nautical_order_items') }}
),

enriched_items as (
    select
        -- Identificadores do Item e Pedido
        ritm.order_item_id
        , ritm.order_id
        , ritm.product_variant_id
        , rord.customer_id
        , rord.location_id
        , rord.employee_id
        , rord.order_number
        , rord.order_subtotal
        , rord.order_discount_amount

        -- Atributos e Status do Pedido
        , rord.order_status
        , rord.order_channel
        , rord.order_status in (
            'cancelled', 'canceled', 'void'
        ) as is_cancelled
        , rord.order_status in (
            'completed', 'paid', 'closed', 'finished', 'confirmed'
        ) as is_completed

        -- Métricas Físicas e Financeiras do Item
        , ritm.quantity
        , ritm.unit_price
        , ritm.line_total

        -- Datas
        , rord.placed_at
        , rord.created_at
        , rord.updated_at

        -- Metadata
        , ritm.ingestion_timestamp
    from order_items as ritm
    inner join orders as rord
        on ritm.order_id = rord.order_id
)

select
    order_item_id
    , order_id
    , product_variant_id
    , customer_id
    , location_id
    , employee_id
    , order_number
    , order_subtotal
    , order_discount_amount
    , order_status
    , order_channel
    , is_cancelled
    , is_completed
    , quantity
    , unit_price
    , line_total
    , placed_at
    , created_at
    , updated_at
    , ingestion_timestamp
from enriched_items