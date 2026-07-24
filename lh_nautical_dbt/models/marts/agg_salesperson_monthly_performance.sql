-- =====================================================================
-- Model: agg_salesperson_monthly_performance
-- Description: Monthly aggregated performance per salesperson, role,
--              and location for HR, targets, and commissions.
-- =====================================================================

with sales_items as (
    select *
    from {{ ref('fct_sales_items') }}
    where employee_sk is not null
),

employees as (
    select *
    from {{ ref('dim_employee') }}
),

aggregated_monthly_sales as (
    select
        -- Groupers & Keys
        fct.employee_sk
        , fct.location_sk
        , emp.employee_id
        , emp.employee_full_name
        , emp.employee_role
        , date_trunc(
            'month'
            , to_date(cast(fct.date_sk as string), 'yyyyMMdd')
        ) as month_start_date

        -- Financial & Quantity Metrics
        , sum(fct.gross_revenue) as total_gross_revenue
        , sum(fct.discount_amount) as total_discount_amount
        , sum(fct.gross_sales_amount) as total_gross_sales_amount
        , sum(fct.refund_amount) as total_refund_amount
        , sum(fct.net_sales_amount) as total_net_sales_amount
        , count(distinct fct.order_id) as total_orders_count
        , sum(fct.net_quantity) as total_net_quantity

    from sales_items as fct
    left join employees as emp
        on fct.employee_sk = emp.employee_sk
    group by
        fct.employee_sk
        , fct.location_sk
        , emp.employee_id
        , emp.employee_full_name
        , emp.employee_role
        , date_trunc(
            'month'
            , to_date(cast(fct.date_sk as string), 'yyyyMMdd')
        )
),

final_aggregation as (
    select
        -- Primary Key para a Tabela Agregada
        {{ dbt_utils.generate_surrogate_key([
            'employee_sk'
            , 'location_sk'
            , 'month_start_date'
        ]) }} as performance_sk

        -- Foreign Keys & Natural Groupers
        , month_start_date
        , employee_sk
        , location_sk
        , employee_id
        , employee_full_name
        , employee_role

        -- Volume & Totais Financeiros
        , total_gross_revenue
        , total_discount_amount
        , total_gross_sales_amount
        , total_refund_amount
        , total_net_sales_amount
        , total_orders_count
        , total_net_quantity

        -- Ticket Médio do Vendedor no Mês
        , round(
            total_net_sales_amount / nullif(total_orders_count, 0)
            , 2
        ) as average_ticket_amount

        -- Metadata de Carga
        , current_timestamp() as aggregated_at
    from aggregated_monthly_sales
)

select
    performance_sk
    , month_start_date
    , employee_sk
    , location_sk
    , employee_id
    , employee_full_name
    , employee_role
    , total_gross_revenue
    , total_discount_amount
    , total_gross_sales_amount
    , total_refund_amount
    , total_net_sales_amount
    , total_orders_count
    , total_net_quantity
    , average_ticket_amount
    , aggregated_at
from final_aggregation
