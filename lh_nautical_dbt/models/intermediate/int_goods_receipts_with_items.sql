-- Step 1: Import staging goods receipts (header level)
with goods_receipts as (
    select *
    from {{ ref('stg_lh_nautical_goods_receipts') }}
)

-- Step 2: Import staging goods receipt items (item level)
, goods_receipt_items as (
    select *
    from {{ ref('stg_lh_nautical_goods_receipt_items') }}
)

-- Step 3: Aggregate receipt items to header level
, receipts_enriched as (
    select
        -- Header identification & metadata pass-through
          goods_receipts.goods_receipt_id
        , goods_receipts.purchase_order_id
        , goods_receipts.received_by_employee_id
        , goods_receipts.received_at
        , goods_receipts.notes-b 

        -- Transformation 1: Count of distinct item lines attached to the receipt
        , count(goods_receipt_items.goods_receipt_item_id) as total_items

        -- Transformation 2: Null-safe total units/volume received matching staging precision (decimal 14,3)
        , cast(
            coalesce(sum(goods_receipt_items.quantity_received), 0) 
          as decimal(14, 3)) as total_quantity_received
          
    from goods_receipts
    left join goods_receipt_items
        on goods_receipts.goods_receipt_id = goods_receipt_items.goods_receipt_id
    group by
          goods_receipts.goods_receipt_id
        , goods_receipts.purchase_order_id
        , goods_receipts.received_by_employee_id
        , goods_receipts.received_at
        , goods_receipts.notes
)

-- Step 4: Final select output
select *
from receipts_enriched