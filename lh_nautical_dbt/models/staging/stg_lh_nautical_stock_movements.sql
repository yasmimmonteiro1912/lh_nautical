with source_data as (

    select *
    from {{ source('lh_nautical', 'stock_movements') }} -- noqa: LT01

)

, renamed as (

    select
        id as stock_movement_id
        , product_variant_id
        , location_id
        , employee_id
        , movement_type
        , reference_table
        , reference_id
        , notes as movement_notes
        , quantity
        , occurred_at
        , ingestion_timestamp
    from source_data

)

select *
from renamed
