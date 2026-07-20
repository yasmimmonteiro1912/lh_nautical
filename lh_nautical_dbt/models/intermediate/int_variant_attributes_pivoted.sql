-- =====================================================================
-- Model: int_lh_nautical_variant_attributes_pivoted
-- Description: Trusted intermediate model that flattens the vertical EAV 
-- structure of variant attributes into a wide horizontal table.
-- Applies try_cast for production resilience against type failures.
-- =====================================================================
with variant_values as (
    select * 
    from {{ ref('stg_lh_nautical_variant_attribute_values') }}
),

attributes as (
    select * 
    from {{ ref('stg_lh_nautical_attributes') }}
),

-- Aqui acontece a mágica do cruzamento
joined_data as (
    select
        vv.product_variant_id
        , vv.attribute_value
        , vv.ingestion_timestamp
        , att.attribute_name
    from variant_values as vv
    left join attributes as att 
        on vv.attribute_id = att.attribute_id
),

pivoted_records as (
    select
        product_variant_id
        -- Agora usamos o NOME do atributo, o que é muito mais seguro!
        , max(
            case
                when lower(trim(attribute_name)) = 'color' then attribute_value
            end
        ) as product_color
        , max(
            case
                when lower(trim(attribute_name)) = 'size' then attribute_value
            end
        ) as product_size
        , try_cast(
            max(
                case
                    when
                        lower(trim(attribute_name)) = 'length_m'
                        then attribute_value
                end
            ) as double
        ) as length_m
        , try_cast(
            max(
                case
                    when
                        lower(trim(attribute_name)) = 'power_hp'
                        then attribute_value
                end
            ) as double
        ) as power_hp
        , try_cast(
            max(
                case
                    when
                        lower(trim(attribute_name)) = 'voltage_v'
                        then attribute_value
                end
            ) as double
        ) as voltage_v
        , try_cast(
            max(
                case
                    when
                        lower(trim(attribute_name)) = 'weight_kg'
                        then attribute_value
                end
            ) as double
        ) as attribute_weight_kg
        , try_cast(
            max(
                case
                    when
                        lower(trim(attribute_name)) = 'waterproof'
                        then attribute_value
                end
            ) as boolean
        ) as is_waterproof
        , max(
            case
                when
                    lower(trim(attribute_name)) = 'material'
                    then attribute_value
            end
        ) as product_material
        , max(ingestion_timestamp) as ingestion_timestamp
    -- Aponta para a CTE onde fizemos o JOIN
    from joined_data
    group by product_variant_id
),

final as (
    select
        product_variant_id
        , product_color
        , product_size
        , length_m
        , power_hp
        , voltage_v
        , attribute_weight_kg
        , is_waterproof
        , product_material
        , ingestion_timestamp
    from pivoted_records
)

select * from final
