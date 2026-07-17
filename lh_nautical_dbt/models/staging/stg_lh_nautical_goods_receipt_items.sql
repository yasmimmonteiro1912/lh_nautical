with source_goods_receipt_items as (

    select * 
    from {{ source('lh_nautical', 'goods_receipt_items') }}

),

renamed as (

    select
        cast(id as int) as goods_receipt_item_id
        , cast(goods_receipt_id as int) as goods_receipt_id
        , cast(purchase_order_item_id as int) as purchase_order_item_id
        , cast(quantity_received as decimal(14, 3)) as quantity_received
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_goods_receipt_items

)

select * 
from renamed
