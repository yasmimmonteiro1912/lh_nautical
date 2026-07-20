with core_variants as (
    select * from {{ ref('int_product_variants_core') }}
),

pivoted_attributes as (
    select * from {{ ref('int_variant_attributes_pivoted') }}
),

final_joined as (
    select
        -- Chave substituta para o Star Schema
        md5(cast(cor.product_variant_id as string)) as variant_sk
        , cor.product_variant_id
        , cor.product_id
        , cor.product_name
        , cor.variant_name
        , cor.brand_name
        , cor.category_name
        , cor.parent_category_name
        , cor.sku
        , cor.barcode_ean
        , cor.sale_price
        , cor.cost_price
        -- Renomeado levemente para não confundir com o weight dos atributos
        , cor.weight_kg as standard_weight_kg
        , cor.is_active
        
        -- Atributos técnicos vindos do pivot
        , att.product_color
        , att.product_size
        , att.length_m
        , att.power_hp
        , att.voltage_v
        , att.attribute_weight_kg
        , att.is_waterproof
        , att.product_material
        
        -- Timestamp de auditoria seguro (se não tiver atributo, usa só o da core)
        , greatest(
            cor.ingestion_timestamp, 
            coalesce(att.ingestion_timestamp, cor.ingestion_timestamp)
        ) as ingestion_timestamp
    from core_variants as cor
    left join pivoted_attributes as att 
        on cor.product_variant_id = att.product_variant_id
)

select * from final_joined