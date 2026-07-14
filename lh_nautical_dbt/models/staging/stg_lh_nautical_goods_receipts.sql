with source_data as (

    select *
    from {{ source('lh_nautical', 'goods_receipts') }}

)

, renamed as (

    select
        cast(id as int) as goods_receipt_id
        , cast(purchase_order_id as int) as purchase_order_id
        , cast(received_by_employee_id as int) as received_by_employee_id
        , cast(received_at as timestamp) as received_at
        , cast(notes as string) as notes
        , cast(created_at as timestamp) as created_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
