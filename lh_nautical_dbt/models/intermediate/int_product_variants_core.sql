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
    where is_preferred = true
),

name_corrections as (
    select * from {{ ref('product_name_corrections') }}
),

valid_prefixes as (
    select collect_list(prefix) as prefixes
    from {{ ref('is_valid_product_name') }}
),

-- Resolve qual correção usar ANTES de aplicar, pegando o match mais
-- específico (typo mais longo) para evitar fanout quando mais de um
-- typo bate como prefixo do mesmo product_name
matched_corrections as (
    select
        prd.product_id
        , corr.typo
        , corr.corrected_name
        , row_number() over (
            partition by prd.product_id
            order by length(corr.typo) desc
        ) as rn
    from products as prd
    inner join name_corrections as corr
        on prd.product_name like corr.typo || '%'
),

best_correction as (
    select product_id, typo, corrected_name
    from matched_corrections
    where rn = 1
),

joined_data as (
    select
        vnt.product_variant_id
        , vnt.product_id
        -- Nome já corrigido, aplicado ANTES da checagem de validade
        , case
            when bc.typo is not null
                then concat(bc.corrected_name, substring(prd.product_name, length(bc.typo) + 1))
            else prd.product_name
          end as product_name
        , vnt.sku as variant_name
        , brd.brand_name
        , cat.category_name
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
    left join categories as pcat
        on cat.parent_category_id = pcat.category_id
    left join preferred_suppliers as sup
        on vnt.product_variant_id = sup.product_variant_id
    left join best_correction as bc
        on prd.product_id = bc.product_id
),

final as (
    select
        jd.*
        -- Validade checada sobre o nome JÁ CORRIGIDO
        , exists(vp.prefixes, p -> jd.product_name like concat(p, '%')) as is_valid_product_name
    from joined_data as jd
    cross join valid_prefixes as vp
)

select * from final