/*
    Model Name: agg_customer_rfm
    
    Business Purpose:
    Aggregates transactional sales data at the customer level to build an 
    RFM (Recency, Frequency, Monetary) segmentation mart for Marketing and CRM.
    
    RFM Mapping:
    - Recency (R): Computed via `last_order_date` and `days_since_last_order`.
    - Frequency (F): Computed via `total_orders` (count of distinct order IDs).
    - Monetary (M): Computed via `lifetime_value` (sum of net revenue) & `average_ticket`.
*/

-- Step 1: Isolate transactional facts and join with dim_dates to fetch the full order date
with sales_items as (
    select
        f.customer_sk
        , f.order_id
        , cast(d.full_date as date) as order_date
        , f.net_revenue
    from {{ ref('fct_sales_items') }} as f
    left join {{ ref('dim_dates') }} as d
        on cast(f.date_sk as string) = cast(d.date_sk as string)
),

-- Step 2: Fetch customer descriptive attributes for filtering/slicing in BI
customers as (
    select
        customer_sk
        , person_type
        , primary_state
    from {{ ref('dim_customers') }}
),

-- Step 3: Join sales with customer metadata and aggregate metrics at customer level
rfm_base as (
    select
        si.customer_sk
        , coalesce(c.person_type, 'N/A') as person_type
        , coalesce(c.primary_state, 'N/A') as primary_state
        -- Recency Core: Finds the latest purchase date
        , max(si.order_date) as last_order_date
        -- Frequency Core: Counts unique orders placed by the customer
        , count(distinct si.order_id) as total_orders
        -- Monetary Core: Sums total net revenue generated (LTV)
        , sum(si.net_revenue) as lifetime_value
    from sales_items as si
    left join customers as c
        on si.customer_sk = c.customer_sk
    group by
        si.customer_sk
        , coalesce(c.person_type, 'N/A')
        , coalesce(c.primary_state, 'N/A')
),

-- Step 4: Compute final derived analytics metrics (Days Since & Average Ticket)
final_rfm as (
    select
        customer_sk
        , person_type
        , primary_state
        , last_order_date
        , current_date() as reference_date
        -- Recency Elapsed Days: Days between execution date and last order
        , datediff(current_date(), last_order_date) as days_since_last_order
        , total_orders
        , lifetime_value
        -- Derived Monetary: Calculates Average Order Value (AOV) with division-by-zero protection
        , case
            when total_orders > 0 then lifetime_value / total_orders
            else 0
          end as average_ticket
    from rfm_base
)

-- Step 5: Final output aligned with dbt project code formatting rules
select
    customer_sk
    , person_type
    , primary_state
    , last_order_date
    , reference_date
    , days_since_last_order
    , total_orders
    , lifetime_value
    , average_ticket
from final_rfm