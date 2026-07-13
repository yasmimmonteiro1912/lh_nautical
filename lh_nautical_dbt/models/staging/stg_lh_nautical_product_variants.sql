with source_data as (

    select *
    from {{ source('lh_nautical', 'product_variants') }}

)

, renamed as (

    select
        cast(id as int) as product_variant_id
        , cast(product_id as int) as product_id
        , cast(sku as string) as sku
        , cast(barcode_ean as string) as barcode_ean
        , cast(sale_price as decimal(14, 2)) as sale_price
        , cast(cost_price as decimal(14, 2)) as cost_price
        , cast(weight_kg as decimal(10, 3)) as weight_kg
        , cast(icms_rate as decimal(5, 2)) as icms_rate
        , cast(ipi_rate as decimal(5, 2)) as ipi_rate
        , cast(is_active as boolean) as is_active
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
