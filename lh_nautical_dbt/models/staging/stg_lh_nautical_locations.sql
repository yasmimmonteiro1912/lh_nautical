with source_data as (

    select *
    from {{ source('lh_nautical', 'locations') }}

)

, renamed as (

    select
        cast(id as int) as location_id
        , cast(name as string) as name
        , cast(location_type as string) as location_type
        , cast(postal_code as string) as postal_code
        , cast(street as string) as street
        , cast(number as string) as number
        , cast(complement as string) as complement
        , cast(district as string) as district
        , cast(city as string) as city
        , cast(state as string) as state
        , cast(country as string) as country
        , cast(is_active as boolean) as is_active
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
