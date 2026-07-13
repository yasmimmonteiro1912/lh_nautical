with source_data as (

    select *
    from {{ source('lh_nautical', 'returns') }} -- noqa: LT01

)

, renamed as (

    select
        cast(id as int) as return_id
        , cast(return_number as string) as return_number
        , cast(customer_id as int) as customer_id
        , cast(order_id as int) as order_id
        , cast(received_at_location_id as int) as received_at_location_id
        , cast(status as string) as return_status
        , cast(reason as string) as return_reason
        , cast(created_at as timestamp) as returned_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
