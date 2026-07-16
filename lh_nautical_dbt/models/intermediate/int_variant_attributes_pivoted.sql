-- =====================================================================
-- Model: int_lh_nautical_variant_attributes_pivoted
-- Description: Trusted intermediate model that flattens the vertical EAV 
--              structure of variant attributes into a wide horizontal table.
--              Applies try_cast for production resilience against type failures.
-- =====================================================================

with source_data as (

    select *
    from {{ ref('stg_lh_nautical_variant_attribute_values') }}

)

-- Leading comma separating CTE blocks
, pivoted_records as (

    select
        product_variant_id
        , max(case when attribute_id = 1 then attribute_value end)
            as product_color
        , max(case when attribute_id = 2 then attribute_value end)
            as product_size
        , try_cast(
            max(case when attribute_id = 3 then attribute_value end) as double
        ) as length_m
        , try_cast(
            max(case when attribute_id = 4 then attribute_value end) as double
        ) as power_hp
        , try_cast(
            max(case when attribute_id = 5 then attribute_value end) as double
        ) as voltage_v
        , try_cast(
            max(case when attribute_id = 6 then attribute_value end) as double
        ) as attribute_weight_kg
        -- Databricks natively and safely converts text 'true'/'false' to boolean
        , try_cast(
            max(case when attribute_id = 7 then attribute_value end) as boolean
        ) as is_waterproof
        , max(case when attribute_id = 8 then attribute_value end)
            as product_material
        , max(ingestion_timestamp) as ingestion_timestamp
    -- Alias 'src' meets the minimum 3-character requirement
    from source_data
    group by product_variant_id

)

-- Final CTE to satisfy the 'select from last CTE' pipeline rule
, final as (

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
    -- Alias 'piv' meets the minimum 3-character requirement
    from pivoted_records

)

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
from final
