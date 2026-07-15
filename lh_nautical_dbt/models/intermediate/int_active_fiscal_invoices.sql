-- =====================================================================
-- Model: int_active_fiscal_invoices
-- Description: Trusted intermediate model that filters active invoices 
--              and zeroes out revenue for cancelled or rejected records.
-- =====================================================================

with staging_invoices as (

    select *
    from {{ ref('stg_lh_nautical_fiscal_invoices') }}

)

-- Leading comma separating CTE blocks
, filtered_active as (

    select
          fiscal_invoice_id
        , order_id
        , nfe_number
        , nfe_access_key
        , nfe_series
        , issued_at
        , invoice_status
        -- Business Rule: Non-authorized invoices do not generate revenue
        , case 
            when invoice_status = 'authorized' then total_amount
            else 0.00
          end as active_total_amount
        , xml_storage_uri
    -- Alias 'sin' meets the minimum 3-character requirement
    from staging_invoices as sin

)

-- Final CTE to satisfy the 'select from last CTE' pipeline rule
, final as (

    select
          fiscal_invoice_id
        , order_id
        , nfe_number
        , nfe_access_key
        , nfe_series
        , issued_at
        , invoice_status
        , active_total_amount
        , xml_storage_uri
    -- Alias 'fac' meets the minimum 3-character requirement
    from filtered_active as fac

)

select
      fiscal_invoice_id
    , order_id
    , nfe_number
    , nfe_access_key
    , nfe_series
    , issued_at
    , invoice_status
    , active_total_amount
    , xml_storage_uri
from final as fin
