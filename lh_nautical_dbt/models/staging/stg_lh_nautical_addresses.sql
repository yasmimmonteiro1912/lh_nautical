with source_addresses as (

    select * 
    from {{ source('lh_nautical', 'addresses') }}

),

renamed as (

    select
        cast(id as int) as address_id
        , cast(customer_id as int) as customer_id
        , cast(address_type as varchar) as address_type
        , cast(postal_code as varchar) as postal_code
        , cast(street as varchar) as street
        , cast(number as varchar) as street_number
        , cast(complement as varchar) as complement
        , cast(district as varchar) as district
        , cast(city as varchar) as city
        , cast(state as varchar) as state
        , cast(country as varchar) as country
        , cast(is_primary as boolean) as is_primary
    from source_addresses

)

select * 
from renamed

