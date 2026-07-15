-- =====================================================================
-- Model: stg_lh_nautical_stock_levels
-- Description: Cleans, standardizes, and formats physical stock levels.
--              - Retains raw negative values to preserve data lineage 
--                and allow business auditability of inventory anomalies.
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'stock_levels') }} -- noqa: LT01

)

, cleaned_records as (

    select
        cast(product_variant_id as int) as product_variant_id
        , cast(location_id as int) as location_id
        , cast(quantity_on_hand as decimal(14,3)) as quantity_on_hand
        , cast(reorder_point as decimal(14,3)) as reorder_point
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select
    product_variant_id
    , location_id
    , quantity_on_hand
    , reorder_point
    , ingestion_timestamp
from cleaned_records
