-- =====================================================================
-- Model: dim_location
-- Layer: Marts / Dimension
-- Description: Core location dimension table representing physical and 
--              operational addresses for analytics consumption.
-- =====================================================================

-- CTE 1: Import clean and transformed location data from intermediate layer
with int_locations as (
    select *
    from {{ ref('int_cep_locations') }}
)

-- CTE 2: Apply final dimensional transformations and business attributes
, transformed as (
    select
        -- Generate Surrogate Key (Dimension PK) from Natural Key (location_id)
        -- Hash key generated inside dbt
        -- to identify dimension records independently of source systems.
        {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_sk

        -- NATURAL KEY (Business/Source Key):
        -- Original identifier coming directly from the source system.
        , location_id

        -- Attribute Mapping
        , location_name
        , location_type
        , postal_code
        , street
        , location_number
        , complement
        , district
        , city
        , location_state 
        , country
        , is_active
        , has_complete_geography
        , is_valid_postal_code
    from int_locations
)

-- Final output selection for downstream analytical models
select *
from transformed
