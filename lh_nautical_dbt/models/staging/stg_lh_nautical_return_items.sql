with source_data as (

    select *
    from {{ source('lh_nautical', 'return_items') }} -- noqa: LT01

)

, renamed as (

    select
        cast(id as int) as return_item_id
        , cast(return_id as int) as return_id
        , cast(order_item_id as int) as order_item_id
        , cast(action as string) as action
        , cast(exchange_variant_id as int) as exchange_variant_id
        , cast(quantity as decimal(14,3)) as quantity
        , cast(unit_refund_amount as decimal(14,2)) as unit_refund_amount
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
