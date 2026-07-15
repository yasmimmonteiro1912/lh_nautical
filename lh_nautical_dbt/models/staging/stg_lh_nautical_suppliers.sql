-- =====================================================================
-- Model: stg_lh_nautical_suppliers
-- Description: Cleans, standardizes, and formats supplier records.
--              - Country-aware phone normalization.
--              - Preserves alphanumeric Tax IDs (VATs).
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'suppliers') }} -- noqa: LT01

)

-- Clean up parentheses, spaces, or hyphens
, cleaned_phone_digits as (

    select
        *
        , regexp_replace(phone, '[^0-9]', '') as clean_phone_digits
    from source_data

)

-- Leading comma separating the CTE blocks to prevent the PARSE_SYNTAX_ERROR
, cleaned_records as (

    select
        cast(id as int) as supplier_id
        , cast(is_active as boolean) as supplier_is_active
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
                
        -- Safe cleaning: Keep alphanumeric to preserve foreign VATs
        , trim(cast(legal_name as string)) as legal_name
        , nullif(trim(cast(trade_name as string)), '') as trade_name
        , nullif(upper(trim(cast(tax_id as string))), '') as tax_id
        , nullif(lower(trim(cast(tax_id_type as string))), '') as tax_id_type
        
        -- Advanced country-aware and prefix-aware phone normalization
        , nullif(trim(cast(contact_name as string)), '') as contact_name
          
        , nullif(lower(trim(cast(email as string))), '') as email
        , case
            when clean_phone_digits is null or clean_phone_digits = '' then null
            
            -- Rule A: BR Supplier OR starts with BR DDI (55)
            when
                upper(trim(cast(country as string))) = 'BR'
                or clean_phone_digits like '55%'
                then
                    case
                    -- Keep special corporate lines (0300/0800)
                        when
                            clean_phone_digits like '0300%'
                            or clean_phone_digits like '0800%'
                            then clean_phone_digits
                        -- Fix '550XX' (DDI + leading zero) to '55XX'
                        when
                            clean_phone_digits like '550%'
                            then concat('55', substring(clean_phone_digits, 4))
                        -- Keep correct international BR standard
                        when
                            clean_phone_digits like '55%'
                            then clean_phone_digits
                        -- Fix '0XX' (leading zero) to '55XX'
                        when
                            clean_phone_digits like '0%'
                            then concat('55', substring(clean_phone_digits, 2))
                        -- Fallback for direct DDD numbers
                        else concat('55', clean_phone_digits)
                    end
            
            -- Rule B: Foreign supplier with local phone
            -- (e.g., Italy landlines starting with '0')
            else clean_phone_digits
        end as phone
        , upper(trim(cast(country as string))) as country
    from cleaned_phone_digits

)

select
    supplier_id
    , legal_name
    , trade_name
    , tax_id
    , tax_id_type
    , contact_name
    , email
    , phone
    , country
    , supplier_is_active
    , ingestion_timestamp
from cleaned_records
