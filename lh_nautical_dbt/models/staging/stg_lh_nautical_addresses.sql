with source_addresses as (

    select * 
    from {{ source('lh_nautical', 'addresses') }}

),

renamed as (

    select
        cast(id as int) as address_id
        , cast(customer_id as int) as customer_id
        , cast(address_type as string) as address_type
        , cast(postal_code as string) as postal_code
        , cast(street as string) as street
        , cast(number as string) as street_number
        , cast(district as string) as district
        , cast(city as string) as city
        , cast(state as string) as customer_state
        , cast(country as string) as country
        , cast(is_primary as boolean) as is_primary
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_addresses

)

select * 
from renamed
