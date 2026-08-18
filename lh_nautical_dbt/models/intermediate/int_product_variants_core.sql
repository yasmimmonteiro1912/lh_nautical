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

-- Agrega os prefixos válidos em um array (1 única linha) para checar via exists(),
-- evitando fanout que um join direto por "like" causaria
valid_prefixes as (
    select collect_list(prefix) as prefixes 
    from {{ ref('is_valid_product_name') }}
),

joined_data as (
    select
        vnt.product_variant_id
        , vnt.product_id
        -- Correção de erros de digitação conhecidos no nome do produto
        -- Match por prefixo (não igualdade exata), pois product_name tem sufixo numérico (ex: "Caboo Náutico 4942")
        , case
            when prd.product_name like corr.typo || '%'
                then concat(corr.corrected_name, substring(prd.product_name, length(corr.typo) + 1))
            else prd.product_name
          end as product_name
        -- Flag: nome do produto começa com algum prefixo reconhecido como válido
        , exists(vp.prefixes, p -> prd.product_name like concat(p, '%')) as is_valid_product_name
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
        on prd.product_name like corr.typo || '%'
    -- cross join seguro: valid_prefixes sempre tem exatamente 1 linha
    cross join valid_prefixes as vp
)

select * from joined_data