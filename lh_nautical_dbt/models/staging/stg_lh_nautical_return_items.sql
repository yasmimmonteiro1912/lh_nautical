with source_data as (

    select *
    from {{ source('lh_nautical', 'return_items') }} -- noqa: LT01

)

, renamed as (

    select
        id as return_item_id
        , return_id
        , order_item_id
        , action
        , exchange_variant_id
        , quantity
        , unit_refund_amount
        , ingestion_timestamp
    from source_data

)

select *
from renamed
