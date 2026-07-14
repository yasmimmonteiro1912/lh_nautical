with source_data as (

    select *
    from {{ source('lh_nautical', 'payments') }}

)

, renamed as (

    select
        cast(id as int) as payment_id
        , cast(order_id as int) as order_id
        , cast(method as string) as payment_method
        , cast(installments as int) as installments
        , cast(amount as decimal(14, 2)) as payment_amount
        , cast(status as string) as payment_status
        , cast(paid_at as timestamp) as paid_at
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
