-- =====================================================================
-- Model: int_cep_locations
-- Layer: Intermediate
-- Description: Standardizes and cleans location data from staging.
--              Applies postal code validation rules and flags 
--              geographic completeness for consumption by dimensions.
-- =====================================================================

-- CTE 1: Imports clean source data from the staging layer
with locations as (
    select *
    from {{ ref('stg_lh_nautical_locations') }}
)

-- CTE 2: 
-- Data transformation, postal code formatting, and quality flags
, transformed as (
    select
        -- Unique Location ID (Natural Key)
        location_id

        -- Basic Business Information
        , location_name
        , location_type
        , is_active

        -- Address Details
        -- (postal_code comes clean, in 8 digits, from staging)
        , postal_code
        , street
        , location_number
        , complement
        , district
        , city
        , location_state
        , country

        -- Audit / Ingestion Metadata
        , ingestion_timestamp

        -- Business Rule: 
        -- Validates whether essential geographic address fields are present
        , (
            postal_code is not null
            and city is not null
            and location_state is not null
        ) as has_complete_geography
        
        -- Business Rule: Postal code is valid if 
        -- it has exactly 8 numeric digits and is not fictitious ('00000000')
        , (
            postal_code is not null
            and length(postal_code) = 8
            and postal_code != '00000000'
        ) as is_valid_postal_code

    from locations
)

-- Final selection of enriched data
-- to make available for downstream layers (Marts/Dim)
select *
from transformed
