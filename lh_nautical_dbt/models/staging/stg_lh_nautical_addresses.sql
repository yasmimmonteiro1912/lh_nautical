with source_data as (

    select *
    from {{ source('lh_nautical', 'addresses') }}

)

, renamed as (

    select
        id as address_id
        , customer_id
        , address_type
        , postal_code
        , street
        , number
    from source_data

)

select *
from renamed
