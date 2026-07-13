with source_data as (

    select *
    from {{ source('lh_nautical', 'returns') }} -- noqa: LT01

)

, renamed as (

    select
        id as return_id
        , return_number
        , customer_id
        , order_id
        , received_at_location_id
        , status as return_status
        , reason as return_reason
        , created_at as returned_at
        , ingestion_timestamp
    from source_data

)

select *
from renamed
