with source_data as (

    select *
    from {{ source('lh_nautical', 'variant_attribute_values') }} -- noqa: LT01

)

, renamed as (

    select
        cast(product_variant_id as int) as product_variant_id
        , cast(attribute_id as int) as attribute_id
        , cast(value as string) as attribute_value
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
