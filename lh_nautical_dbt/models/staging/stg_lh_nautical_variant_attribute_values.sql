with source_data as (

    select *
    from {{ source('lh_nautical', 'variant_attribute_values') }} -- noqa: LT01

)

, renamed as (

    select
        product_variant_id
        , attribute_id
        , value as attribute_value
        , ingestion_timestamp
    from source_data

)

select *
from renamed
