-- =====================================================================
-- Model: stg_lh_nautical_purchase_order_items
-- Description: Cleans, standardizes, and formats purchase order items.
--              - Ensures proper decimal representation for unit costs
--                and quantities.
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'purchase_order_items') }} -- noqa: LT01

)

-- Leading comma separating the CTE blocks to prevent the PARSE_SYNTAX_ERROR
, cleaned_records as (

    select
        cast(id as int) as purchase_order_item_id
        , cast(purchase_order_id as int) as purchase_order_id
        , cast(product_variant_id as int) as product_variant_id
        , cast(quantity_ordered as decimal(14,3)) as quantity_ordered
        , cast(unit_cost as decimal(14,3)) as unit_cost
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select
    purchase_order_item_id
    , purchase_order_id
    , product_variant_id
    , quantity_ordered
    , unit_cost
    , ingestion_timestamp
from cleaned_records
