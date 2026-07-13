with source_data as (

    select *
    from {{ source('lh_nautical', 'purchase_order_items') }} -- noqa: LT01

)

, renamed as (

    select
        id as purchase_order_item_id
        , purchase_order_id
        , product_variant_id
        , quantity_ordered
        , unit_cost as unit_cost_original
        , ingestion_timestamp
    from source_data

)

select *
from renamed
