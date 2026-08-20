with core_variants as (
    select * from {{ ref('int_product_variants_core') }}
),

pivoted_attributes as (
    select * from {{ ref('int_variant_attributes_pivoted') }}
),

prepared_data as (
    select
        cor.product_variant_id
        , cor.product_id
        -- Produtos com nome inválido/placeholder (ex: "Cliente Genérico", "TBD") recebem rótulo genérico
        -- em vez de serem descartados, preservando a linha para o estoque associado a eles
        , case
            when cor.is_valid_product_name then cor.product_name
            else 'Produto Não Identificado'
          end as product_name
        , cor.is_valid_product_name
        , cor.variant_name
        , cor.brand_name
        , cor.category_name
        , cor.parent_category_name
        , cor.sku
        , cor.barcode_ean
        , cor.sale_price
        , cor.cost_price
        , cor.weight_kg as standard_weight_kg
        , cor.is_active
        -- Campos novos do fornecedor preferencial trazidos da core
        , cor.preferred_supplier_id
        , cor.preferred_supplier_lead_time
        -- Atributos técnicos pivotados
        , att.product_color
        , att.product_size
        , att.length_m
        , att.power_hp
        , att.voltage_v
        , att.attribute_weight_kg
        , att.is_waterproof
        , att.product_material

        -- 2. Cálculos, funções e transformações ficam por último na CTE
        , md5(cast(cor.product_variant_id as string)) as variant_sk
        , case
            when att.ingestion_timestamp > cor.ingestion_timestamp
                then att.ingestion_timestamp
            else cor.ingestion_timestamp
        end as ingestion_timestamp
    from core_variants as cor
    left join pivoted_attributes as att
        on cor.product_variant_id = att.product_variant_id
)

-- 3. No SELECT final organizamos a casa colocando as chaves no topo para o BI
select
    variant_sk
    , product_variant_id
    , product_id
    , product_name
    , is_valid_product_name
    , variant_name
    , brand_name
    , category_name
    , parent_category_name
    , sku
    , barcode_ean
    , sale_price
    , cost_price
    , standard_weight_kg
    , is_active
    , preferred_supplier_id
    , preferred_supplier_lead_time
    , product_color
    , product_size
    , length_m
    , power_hp
    , voltage_v
    , attribute_weight_kg
    , is_waterproof
    , product_material
    , ingestion_timestamp
from prepared_data