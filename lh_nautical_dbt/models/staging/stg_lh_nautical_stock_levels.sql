with source_data as (

    select *
    from {{ source('lh_nautical', 'stock_levels') }} -- noqa: LT01

)

, renamed as (

    select
        product_variant_id
        , location_id
        , quantity_on_hand
        , reorder_point
        , ingestion_timestamp
    from source_data

)

select *
from renamed
