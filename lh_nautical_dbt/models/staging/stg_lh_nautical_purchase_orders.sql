with source_data as (

    select *
    from {{ source('lh_nautical', 'purchase_orders') }} -- noqa: LT01

)

, renamed as (

    select
        cast(id as int) as purchase_order_id
        , cast(po_number as string) as po_number
        , cast(supplier_id as int) as supplier_id
        , cast(buyer_id as int) as buyer_id
        , cast(destination_location_id as int) as destination_location_id
        , cast(status as string) as purchase_order_status
        , cast(currency as string) as purchase_currency
        , cast(placed_at as timestamp) as placed_at
        , cast(expected_delivery_at as date) as expected_delivery_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
