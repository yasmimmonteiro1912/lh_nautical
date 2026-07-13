with source_data as (

    select *
    from {{ source('lh_nautical', 'stock_levels') }} -- noqa: LT01

)

, renamed as (

    select
        cast(product_variant_id as int) as product_variant_id
        , cast(location_id as int) as location_id
        , cast(quantity_on_hand as decimal(14,3)) as quantity_on_hand
        , cast(reorder_point as decimal(14,3)) as reorder_point
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
