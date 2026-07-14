with source_data as (

    select *
    from {{ source('lh_nautical', 'purchase_order_items') }} -- noqa: LT01

)

, renamed as (

    select
        cast(id as int) as purchase_order_item_id
        , cast(purchase_order_id as int) as purchase_order_id
        , cast(product_variant_id as int) as product_variant_id
        , cast(quantity_ordered as decimal(14,3)) as quantity_ordered
        , cast(unit_cost as decimal(14,2)) as unit_cost_original
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
