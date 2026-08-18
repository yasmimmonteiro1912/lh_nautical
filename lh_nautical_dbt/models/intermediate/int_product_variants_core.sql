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

preferred_suppliers as (
    select * 
    from {{ ref('stg_lh_nautical_product_suppliers') }}
    -- Filtro estratégico para garantir granularidade de 1 linha por SKU
    where is_preferred = true
),

name_corrections as (
    select * from {{ ref('product_name_corrections') }}
),

joined_data as (
    select
        vnt.product_variant_id
        , vnt.product_id
        -- Correção de erros de digitação conhecidos no nome do produto
        , coalesce(corr.corrected_name, prd.product_name) as product_name
        , vnt.sku as variant_name
        , brd.brand_name
        , cat.category_name
        -- Buscamos o nome da categoria pai através da tabela 'pcat'
        , pcat.category_name as parent_category_name
        , vnt.sku
        , vnt.barcode_ean
        , vnt.sale_price
        , vnt.cost_price
        , vnt.weight_kg
        , vnt.is_active
        , sup.supplier_id as preferred_supplier_id
        , sup.lead_time_days as preferred_supplier_lead_time
        , vnt.ingestion_timestamp
    from variants as vnt
    left join products as prd 
        on vnt.product_id = prd.product_id
    left join brands as brd 
        on prd.brand_id = brd.brand_id
    left join categories as cat 
        on prd.category_id = cat.category_id
    -- Self-join para descobrir quem é o pai da categoria atual
    left join categories as pcat 
        on cat.parent_category_id = pcat.category_id
    left join preferred_suppliers as sup 
        on vnt.product_variant_id = sup.product_variant_id
    left join name_corrections as corr
        on trim(prd.product_name) = corr.typo
)

select * from joined_data