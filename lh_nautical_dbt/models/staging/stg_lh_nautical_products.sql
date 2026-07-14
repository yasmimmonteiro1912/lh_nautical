with source_data as (

    select *
    from {{ source('lh_nautical', 'products') }}

)

, renamed as (

    select
        cast(id as int) as product_id
        , cast(name as string) as product_name
        , cast(description as string) as product_description
        , cast(brand_id as int) as brand_id
        , cast(category_id as int) as category_id
        , cast(ncm_code as string) as ncm_code
        , cast(unit_of_measure as string) as unit_of_measure
        , cast(is_active as boolean) as is_active
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
