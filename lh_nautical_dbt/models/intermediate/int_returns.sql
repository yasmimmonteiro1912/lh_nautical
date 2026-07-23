-- =====================================================================
-- Model: int_returns_with_items
-- Description: Intermediate model combining return headers with line items
--              to calculate total returned quantities and refund amounts.
-- =====================================================================

with returns_header as (
    select
        return_id
        , return_number
        , customer_id
        , order_id
        , received_at_location_id
        , lower(trim(return_status)) as return_status
        , lower(trim(return_reason)) as return_reason
        , returned_at
        , ingestion_timestamp
    from {{ ref('stg_lh_nautical_returns') }}
),

returns_items as (
    select
        return_item_id
        , return_id
        , order_item_id
        , return_action
        , exchange_variant_id
        , quantity
        , unit_refund_amount
    from {{ ref('stg_lh_nautical_return_items') }}
),

returns_enriched as (
    select
        -- Identificação do Cabeçalho
        retn.return_id
        , retn.return_number
        , retn.customer_id
        , retn.order_id
        , retn.received_at_location_id
        , retn.return_status
        , retn.return_reason
        , retn.returned_at
        , retn.ingestion_timestamp

        -- Métricas Agregadas dos Itens
        , cast(
            coalesce(sum(ritm.quantity), 0)
            as decimal(14, 3)
        ) as total_returned_quantity

        , cast(
            coalesce(
                sum(ritm.quantity * ritm.unit_refund_amount)
                , 0
            )
            as decimal(14, 2)
        ) as total_refund_amount

        , count(ritm.return_item_id) as total_returned_items

    from returns_header as retn
    left join returns_items as ritm
        on retn.return_id = ritm.return_id
    group by
        retn.return_id
        , retn.return_number
        , retn.customer_id
        , retn.order_id
        , retn.received_at_location_id
        , retn.return_status
        , retn.return_reason
        , retn.returned_at
        , retn.ingestion_timestamp
)

select
    return_id
    , return_number
    , customer_id
    , order_id
    , received_at_location_id
    , return_status
    , return_reason
    , returned_at
    , total_returned_quantity
    , total_refund_amount
    , total_returned_items
    , ingestion_timestamp
from returns_enriched