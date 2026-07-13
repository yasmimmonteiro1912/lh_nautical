with source_data as (

    select *
    from {{ source('lh_nautical', 'purchase_orders') }} -- noqa: LT01

)

, renamed as (

    select
        id as purchase_order_id
        , po_number
        , supplier_id
        , buyer_id
        , destination_location_id
        , status as purchase_order_status
        , currency as purchase_currency
        , placed_at
        , expected_delivery_at
        , ingestion_timestamp
    from source_data

)

select *
from renamed
