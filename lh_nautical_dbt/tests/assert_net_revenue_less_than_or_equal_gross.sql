/*
    Test Name: assert_net_revenue_less_than_or_equal_gross
    
    Business Rule:
    Net revenue (line item total after discounts) should never exceed 
    Gross revenue (quantity * unit price). If Net > Gross, it indicates 
    a calculation error, inverted discount logic, or corrupted source data.
    
    dbt Test Behavior:
    dbt evaluates singular tests by running the query. 
    - 0 rows returned = PASS (Data quality intact)
    - 1+ rows returned = FAIL (Data anomaly detected)
*/

-- Step 1: Isolate only the required columns 
-- from the Fact table to optimize query execution
with sales_items as (
    select
        order_id
        , customer_sk
        , net_revenue
        , gross_revenue
    from {{ ref('fct_sales_items') }}
)

-- Step 2: Query for records that violate the financial business rule
select
    order_id
    , customer_sk
    , net_revenue
    , gross_revenue
from sales_items
-- Failure Condition: 
-- Filter for rows where Net Revenue illegally surpasses Gross Revenue
where net_revenue > gross_revenue
