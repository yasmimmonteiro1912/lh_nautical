with source_data as (

    select *
    from {{ source('lh_nautical', 'order_items') }}

)

, renamed as (

    select
        cast(id as int) as order_item_id
        , cast(order_id as int) as order_id
        , cast(product_variant_id as int) as product_variant_id
        , cast(quantity as int) as quantity
        , cast(unit_price as decimal(14, 2)) as unit_price
        , cast(icms_rate as decimal(5, 2)) as icms_rate
        , cast(ipi_rate as decimal(5, 2)) as ipi_rate
        , cast(line_total as decimal(14, 2)) as line_total
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
