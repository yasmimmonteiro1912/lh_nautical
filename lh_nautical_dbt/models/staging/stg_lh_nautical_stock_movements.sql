with source_data as (

    select *
    from {{ source('lh_nautical', 'stock_movements') }} -- noqa: LT01

)

, renamed as (

    select
        cast(id as bigint) as stock_movement_id
        , cast(product_variant_id as int) as product_variant_id
        , cast(location_id as int) as location_id
        , cast(employee_id as int) as employee_id
        , cast(movement_type as string) as movement_type
        , cast(reference_table as string) as reference_table
        , cast(reference_id as int) as reference_id
        , cast(notes as string) as movement_notes
        , cast(quantity as decimal(14,3)) as quantity
        , cast(occurred_at as timestamp) as occurred_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
