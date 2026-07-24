-- =====================================================================
-- Test: test_net_revenue_less_than_or_equal_gross
-- Description: Garante que a receita líquida nunca seja maior 
--              que a receita bruta de tabela na fct_sales_items.
-- =====================================================================

select
    order_item_id
    , order_id
    , net_sales_amount
    , gross_revenue
from {{ ref('fct_sales_items') }}
where net_sales_amount > gross_revenue