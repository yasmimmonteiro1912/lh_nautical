/*
  =============================================================================
  MODEL: int_sku_supplier
  LAYER: Intermediate

  PURPOSE:
    Consolidates and sanitizes the relationship between SKUs and their
    respective suppliers sourced from `stg_lh_nautical_product_suppliers`.

  BUSINESS RULES & DATA CLEANING:
    1. Expected Supplier SKU Pattern: SUP-XXX-XXXXX (e.g., SUP-018-00112).
    2. Normalizes any noise, hyphens, placeholders, or terms indicating
       missing data (e.g., '—', '?', 'TODO', 'SEM-CODIGO') into NULL.
    3. Standardizes valid SKUs using UPPER() and TRIM() to remove unexpected
       whitespaces and ensure consistency across downstream JOINs.
  =============================================================================
*/

with supplier_sku_source as (
    select *
    from {{ ref('stg_lh_nautical_product_suppliers') }}
)

, cleaned as (
    select
        product_variant_id
        , supplier_id
        , last_quoted_cost
        , lead_time_days
        , is_preferred

        -- --------------------------------------------------------------------
        -- Handling of `supplier_sku`:
        -- Converts noise, placeholders, and bad entries into NULL.
        -- Expected standard format: 'SUP-XXX-XXXXX'
        -- --------------------------------------------------------------------
        , case
            -- 1. Handle empty strings, whitespace-only entries, or NULLs
            when trim(coalesce(supplier_sku, '')) = ''
                then null

            -- 2. Handle known noise patterns and placeholders
            when
                lower(trim(supplier_sku)) in (
                    '-'
                    , '--'
                    , '—'
                    , '?'
                    , 'sem-codigo'
                    , 'sem código'
                    , 'tbd'
                    , 'todo'
                    , 'nao informado'
                    , 'não informado'
                    , 'null'
                    , 'none'
                )
                then null

            -- 3. VALID SKU: Applies TRIM and converts to UPPERCASE
            else upper(trim(supplier_sku))
        end as supplier_sku

    from supplier_sku_source
)

select *
from cleaned
