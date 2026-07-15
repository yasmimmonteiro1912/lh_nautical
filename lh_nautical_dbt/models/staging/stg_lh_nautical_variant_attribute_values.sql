-- =====================================================================
-- Model: stg_lh_nautical_variant_attribute_values
-- Description: Cleans, standardizes, and formats product variant 
--              EAV values.
--              - Converts dirty placeholders (e.g., '.', '??', 
--                'test', 'n/a') to NULL.
--              - Normalizes decimal separator from comma to dot.
--              - Ensures lowercase boolean strings.
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'variant_attribute_values') }}

)

-- Leading comma separating the CTE blocks to prevent the PARSE_SYNTAX_ERROR
, cleaned_records as (

    select
        cast(product_variant_id as int) as product_variant_id
        , cast(attribute_id as int) as attribute_id
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
        , case
            -- 1. Coerce known garbage placeholders to NULL
            when
                lower(trim(value)) in (
                    '.'
                    , '??'
                    , 'test'
                    , 'null'
                    , 'none'
                    , ''
                    , 'n/a'
                    , 'na'
                    , 'fixme'
                    , 'asdf'
                )
                then null

            -- 2. Standardize boolean string representations
            when lower(trim(value)) = 'true' then 'true'
            when lower(trim(value)) = 'false' then 'false'

            -- 3. Replace Portuguese comma decimal separators with dots
            else replace(trim(value), ',', '.')
        end as attribute_value
    from source_data

)

select
    product_variant_id
    , attribute_id
    , attribute_value
    , ingestion_timestamp
from cleaned_records
