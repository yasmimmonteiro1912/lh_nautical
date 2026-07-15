-- =====================================================================
-- Model: fct_sales_items
-- Description: Fact table containing item-level sales transactions,
--              resolved with surrogate keys and enriched with active 
--              fiscal invoice metadata.
-- =====================================================================

with staging_order_items as (

    select *
    from {{ ref('stg_lh_nautical_order_items') }}

)

-- Leading comma to separate CTE blocks
, staging_orders as (

    select *
    from {{ ref('stg_lh_nautical_orders') }}

)

, intermediate_fiscal_invoices as (

    select *
    from {{ ref('int_active_fiscal_invoices') }}

)

, joined_sales_data as (

    select
          itm.order_item_id
        , itm.order_id
        , ord.order_number
        , ord.channel
        , ord.customer_id
        -- Product variant lives on the item level
        , itm.product_variant_id as variant_id
        , ord.location_id
        -- Salesperson mapped to employee dimension
        , ord.salesperson_id as employee_id
        -- Extracted date from timestamp for date_sk mapping
        , cast(ord.placed_at as date) as order_date
        , itm.quantity
        , itm.unit_price
        -- Discount captured at order level
        , coalesce(ord.discount_amount, 0.00) as discount_amount
        -- Revenue calculations
        , (itm.quantity * itm.unit_price) as gross_revenue
        , itm.line_total as net_revenue
        -- Fiscal invoice details from intermediate layer
        , inv.nfe_number
        , inv.nfe_series
        , inv.invoice_status
        , coalesce(inv.active_total_amount, 0.00) as invoice_total_amount
    from staging_order_items as itm
    left join staging_orders as ord
        on itm.order_id = ord.order_id
    left join intermediate_fiscal_invoices as inv
        on itm.order_id = inv.order_id

)

, surrogate_keys_generation as (

    select
          -- Generating Surrogate Keys using dbt_utils macro
          {{ dbt_utils.generate_surrogate_key(['order_item_id']) }} as sales_item_sk
        , {{ dbt_utils.generate_surrogate_key(['order_date']) }} as date_sk
        , {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk
        , {{ dbt_utils.generate_surrogate_key(['variant_id']) }} as variant_sk
        , {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_sk
        , {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_sk
        , order_id
        , order_number
        , channel
        , quantity
        , unit_price
        , discount_amount
        , gross_revenue
        , net_revenue
        , nfe_number
        , nfe_series
        , invoice_status
        , invoice_total_amount
    from joined_sales_data as jsd

)

-- Final CTE to ensure linear query compilation and style guides
, final as (

    select
          sales_item_sk
        , date_sk
        , customer_sk
        , variant_sk
        , location_sk
        , employee_sk
        , order_id
        , order_number
        , channel
        , quantity
        , unit_price
        , discount_amount
        , gross_revenue
        , net_revenue
        , nfe_number
        , nfe_series
        , invoice_status
        , invoice_total_amount
    from surrogate_keys_generation as skg

)

select
      sales_item_sk
    , date_sk
    , customer_sk
    , variant_sk
    , location_sk
    , employee_sk
    , order_id
    , order_number
    , channel
    , quantity
    , unit_price
    , discount_amount
    , gross_revenue
    , net_revenue
    , nfe_number
    , nfe_series
    , invoice_status
    , invoice_total_amount
from final as fin