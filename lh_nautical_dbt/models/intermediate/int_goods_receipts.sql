-- Step 1: Import and sanitize staging goods receipts (header level)
with goods_receipts as (
    select 
        goods_receipt_id
        , purchase_order_id
        , received_by_employee_id
        , received_at
        
        -- SANITIZAÇÃO: Padronização dos comentários operacionais
        , case
            when notes is null or trim(notes) in ('', '-', '—', '...') 
                then 'sem_nota'
            when lower(trim(notes)) in ('ok', 'ok!', 'conferido', 'tudo certo') 
                then 'conferido'
            when
                lower(trim(notes)) in (
                    'checar'
                    , 'ver com geren...'
                    , 'ver com gerente'
                    , 'necessita_revisao'
                ) 
                then 'necessita_revisao'
            else lower(trim(notes))
        end as sanitized_notes

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
        
        -- Trazendo a nota já tratada e padronizada
        , goods_receipts.sanitized_notes as notes 

        -- Transformation 1: Null-safe total units/volume received 
        -- matching staging precision (decimal 14,3)
        , cast(
            coalesce(sum(goods_receipt_items.quantity_received), 0) 
            as decimal(14, 3)
        ) as total_quantity_received 

        -- Transformation 2: Count of distinct item lines attached to receipt
        , count(goods_receipt_items.goods_receipt_item_id) as total_items
          
    from goods_receipts
    left join goods_receipt_items
        on
            goods_receipts.goods_receipt_id
            = goods_receipt_items.goods_receipt_id
    group by
        goods_receipts.goods_receipt_id
        , goods_receipts.purchase_order_id
        , goods_receipts.received_by_employee_id
        , goods_receipts.received_at
        , goods_receipts.sanitized_notes
)

-- Step 4: Final select output
select *
from receipts_enriched
