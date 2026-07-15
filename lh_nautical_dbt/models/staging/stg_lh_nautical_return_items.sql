-- =====================================================================
-- Model: stg_lh_nautical_return_items
-- Description: Cleans, standardizes, and formats returned items.
--              - Standardizes return action casings to lowercase.
--              - Casts metrics to appropriate decimal types.
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'return_items') }} -- noqa: LT01

)

-- Leading comma separating the CTE blocks to prevent the PARSE_SYNTAX_ERROR
, cleaned_records as (

    select
        cast(id as int) as return_item_id
        , cast(return_id as int) as return_id
        , cast(order_item_id as int) as order_item_id
        , cast(exchange_variant_id as int) as exchange_variant_id
        , cast(quantity as decimal(14,3)) as quantity
        , cast(unit_refund_amount as decimal(14,2)) as unit_refund_amount
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
        , lower(trim(cast(action as string))) as return_action
    from source_data

)

select
    return_item_id
    , return_id
    , order_item_id
    , return_action
    , exchange_variant_id
    , quantity
    , unit_refund_amount
    , ingestion_timestamp
from cleaned_records
