-- =====================================================================
-- Model: fct_purchase_orders
-- Description: Fact table recording purchase orders, currency conversions,
--              supplier fulfillment SLA (lead times), and physical receipts.
-- =====================================================================

with orders as (
    select * from {{ ref('int_purchase_orders_converted') }}
),

receipts as (
    select * from {{ ref('int_goods_receipts') }}
),

joined_orders as (
    select
        -- 1. Chaves Naturais e Dimensões Degeneradas
        pord.purchase_order_id
        , pord.po_number
        , pord.supplier_id
        , pord.buyer_id
        , pord.destination_location_id
        , rcpt.goods_receipt_id
        , rcpt.received_by_employee_id
        , pord.purchase_order_status
        , pord.purchase_currency
        , coalesce(rcpt.notes, 'pendente_de_entrega') as receipt_status

        -- 2. Datas e Timestamps
        , pord.placed_at
        , pord.expected_delivery_at
        , rcpt.received_at
        , pord.ingestion_timestamp

        -- 3. Métricas de SLA e Lead Time de Entrega
        , case 
            when rcpt.received_at is not null 
                then datediff(rcpt.received_at, pord.placed_at)
        end as lead_time_days

        , case 
            when rcpt.received_at is not null 
                then datediff(rcpt.received_at, pord.expected_delivery_at)
        end as delivery_delay_days

        -- 4. Métricas Físicas e Financeiras
        , pord.applied_exchange_rate
        , coalesce(rcpt.total_quantity_received, 0) as total_quantity_received
        , coalesce(rcpt.total_items, 0) as total_received_items

        -- 5. Surrogate Keys
        , {{ dbt_utils.generate_surrogate_key(['pord.purchase_order_id']) }}
            as purchase_order_sk

        , {{ dbt_utils.generate_surrogate_key(['pord.supplier_id']) }}
            as supplier_sk

        , {{ dbt_utils.generate_surrogate_key([
            'pord.destination_location_id'
        ]) }} as location_sk

        , case
            when pord.buyer_id is not null
                then {{ dbt_utils.generate_surrogate_key(['pord.buyer_id']) }}
        end as buyer_sk

        , case
            when rcpt.goods_receipt_id is not null
                then {{ dbt_utils.generate_surrogate_key([
                    'rcpt.goods_receipt_id'
                ]) }}
        end as goods_receipt_sk
        , cast(date_format(pord.placed_at, 'yyyyMMdd') as int) as date_sk

    from orders as pord
    left join receipts as rcpt
        on pord.purchase_order_id = rcpt.purchase_order_id
)

select
    -- Primary Key
    purchase_order_sk

    -- Foreign Keys (Star Schema Dimensions)
    , date_sk
    , supplier_sk
    , location_sk
    , buyer_sk
    , goods_receipt_sk

    -- Natural Keys / Degenerate Dimensions
    , purchase_order_id
    , po_number
    , supplier_id
    , buyer_id
    , destination_location_id
    , goods_receipt_id
    , received_by_employee_id
    , purchase_order_status
    , purchase_currency
    , receipt_status

    -- Dates
    , placed_at
    , expected_delivery_at
    , received_at

    -- SLA & Measures
    , lead_time_days
    , delivery_delay_days
    , applied_exchange_rate
    , total_quantity_received
    , total_received_items

    -- Metadata
    , ingestion_timestamp
from joined_orders
