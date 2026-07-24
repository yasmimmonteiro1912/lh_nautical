-- =====================================================================
-- Model: int_returns_with_items
-- Description: Intermediate model combining return headers with line items
--              and applying business rules for cancelled returns.
-- =====================================================================

with returns_header as (
    select
        return_id
        , return_number
        , customer_id
        , order_id
        , received_at_location_id
        , lower(trim(return_status)) as return_status
        , coalesce(
            lower(trim(return_reason)), 'nao_informado'
        ) as return_reason
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

        -- Flags de Negócio
        , retn.return_status = 'completed' as is_completed_return
        , retn.return_status = 'cancelled' as is_cancelled_return

        -- Métricas Brutas (Para Auditoria)
        , cast(
            coalesce(sum(ritm.quantity), 0)
            as decimal(14, 3)
        ) as gross_returned_quantity

        , cast(
            coalesce(
                sum(ritm.quantity * ritm.unit_refund_amount)
                , 0
            )
            as decimal(14, 2)
        ) as gross_refund_amount

        -- Métricas Efetivas (Zera se a devolução foi cancelada)
        , case
            when retn.return_status = 'cancelled' then 0
            else cast(
                coalesce(sum(ritm.quantity), 0)
                as decimal(14, 3)
            )
        end as total_returned_quantity

        , case
            when retn.return_status = 'cancelled' then 0
            else cast(
                coalesce(
                    sum(ritm.quantity * ritm.unit_refund_amount)
                    , 0
                )
                as decimal(14, 2)
            )
        end as total_refund_amount

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
    , is_completed_return
    , is_cancelled_return
    , gross_returned_quantity
    , gross_refund_amount
    , total_returned_quantity
    , total_refund_amount
    , total_returned_items
    , ingestion_timestamp
from returns_enriched