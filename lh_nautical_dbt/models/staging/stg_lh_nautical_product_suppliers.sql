with source_data as (

    select *
    from {{ source('lh_nautical', 'product_suppliers') }}

)

, renamed as (

    select
        cast(product_variant_id as int) as product_variant_id
        , cast(supplier_id as int) as supplier_id
        , cast(supplier_sku as string) as supplier_sku
        , cast(last_quoted_cost as decimal(14, 2)) as last_quoted_cost
        , cast(lead_time_days as int) as lead_time_days
        , cast(is_preferred as boolean) as is_preferred
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
