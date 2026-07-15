-- =====================================================================
-- Model: stg_lh_nautical_purchase_orders
-- Description: Cleans, standardizes, and formats purchase order headers.
--              - Standardizes currency codes to uppercase and trims spaces.
--              - Standardizes status values to lowercase.
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'purchase_orders') }} -- noqa: LT01

)

-- Leading comma separating the CTE blocks to prevent the PARSE_SYNTAX_ERROR
, cleaned_records as (

    select
        cast(id as int) as purchase_order_id
        , cast(supplier_id as int) as supplier_id
        , cast(buyer_id as int) as buyer_id
        , cast(destination_location_id as int) as destination_location_id
        , cast(placed_at as timestamp) as placed_at
        , cast(expected_delivery_at as timestamp) as expected_delivery_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
        , trim(cast(po_number as string)) as po_number
        , lower(trim(cast(status as string))) as purchase_order_status
        , upper(trim(cast(currency as string))) as purchase_currency
    from source_data

)

select
    purchase_order_id
    , po_number
    , supplier_id
    , buyer_id
    , destination_location_id
    , purchase_order_status
    , purchase_currency
    , placed_at
    , expected_delivery_at
    , ingestion_timestamp
from cleaned_records
