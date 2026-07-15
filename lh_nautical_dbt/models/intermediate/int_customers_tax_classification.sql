-- =====================================================================
-- Model: int_customers_tax_classification
-- Description: Classifies customers based on tax_id (CPF/CNPJ) 
--              and state_registration (Contributor vs Non-contributor/Exempt).
-- =====================================================================

with staging_customers as (

    select *
    from {{ ref('stg_lh_nautical_customers') }}

)

-- Leading comma to separate CTE blocks
, tax_classification as (

    select
          customer_id
        , person_type
        , tax_id
        , state_registration
        -- Determining tax identification type based on string length
        , case
            when length(trim(tax_id)) = 11 then 'CPF'
            when length(trim(tax_id)) = 14 then 'CNPJ'
            else 'INVALID_TAX_ID'
          end as tax_id_type
        -- Identifying state registration tax status (exempt or active contributor)
        , case
            when state_registration is null then 'Non-Contributor'
            when upper(trim(state_registration)) = 'ISENTO' then 'Exempt'
            else 'Contributor'
          end as state_tax_status
    -- Alias 'cus' has exactly 3 characters
    from staging_customers as cus

)

-- Added final CTE to ensure the end select strictly queries the last CTE
, final as (

    select
          customer_id
        , person_type
        , tax_id
        , tax_id_type
        , state_registration
        , state_tax_status
    -- Alias 'tax' has exactly 3 characters
    from tax_classification as tax

)

select
      customer_id
    , person_type
    , tax_id
    , tax_id_type
    , state_registration
    , state_tax_status
-- The final select strictly queries the 'final' CTE using a compliant alias
from final 