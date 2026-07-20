with variants as (
    select * from {{ ref('stg_lh_nautical_product_variants') }}
),

products as (
    select * from {{ ref('stg_lh_nautical_products') }}
),

brands as (
    select * from {{ ref('stg_lh_nautical_brands') }}
),

categories as (
    select * from {{ ref('stg_lh_nautical_categories') }}
),

joined_data as (
    select
        var.product_variant_id
        , var.product_id
        , prd.product_name
        , var.sku as variant_name
        , brd.brand_name
        , cat.category_name
        -- Buscamos o nome da categoria pai através da tabela 'pcat'
        , pcat.category_name as parent_category_name
        , var.sku
        , var.barcode_ean
        , var.sale_price
        , var.cost_price
        , var.weight_kg
        , var.is_active
        , var.ingestion_timestamp
    from variants as var
    left join products as prd 
        on var.product_id = prd.product_id
    left join brands as brd 
        on prd.brand_id = brd.brand_id
    left join categories as cat 
        on prd.category_id = cat.category_id
    -- Self-join para descobrir quem é o pai da categoria atual
    left join categories as pcat 
        on cat.parent_category_id = pcat.category_id
)

select * from joined_data