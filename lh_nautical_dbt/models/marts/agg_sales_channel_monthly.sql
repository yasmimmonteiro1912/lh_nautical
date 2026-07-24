-- =====================================================================
-- Model: agg_sales_channel_monthly
-- Description: Executive monthly aggregated sales report by channel,
--              state, and city for financial and executive management.
-- =====================================================================

with sales_items as (
    select *
    from {{ ref('fct_sales_items') }}
),

locations as (
    select *
    from {{ ref('dim_location') }}
),

aggregated_sales as (
    select
        -- Agrupadores Temporais, de Canal e Geográficos
        fct.channel
        , fct.location_sk
        , date_trunc(
            'month'
            , to_date(cast(fct.date_sk as string), 'yyyyMMdd')
        ) as month_start_date
        , coalesce(loc.location_state, 'NÃO INFORMADO') as location_state
        , coalesce(loc.city, 'NÃO INFORMADO') as city

        -- Métricas Financeiras e de Volume
        , sum(fct.gross_revenue) as total_gross_revenue
        , sum(fct.discount_amount) as total_discount_amount
        , sum(fct.gross_sales_amount) as total_gross_sales_amount
        , sum(fct.refund_amount) as total_refund_amount
        , sum(fct.net_sales_amount) as total_net_sales_amount
        , sum(fct.quantity) as total_quantity_sold
        , sum(fct.net_quantity) as total_net_quantity_sold
        , count(distinct fct.order_id) as total_orders_count

        -- Total Faturado em Notas Fiscais Ativas
        , sum(
            case
                when fct.nfe_number is not null
                    then fct.gross_sales_amount
                else 0
            end
        ) as total_active_invoiced_amount

    from sales_items as fct
    left join locations as loc
        on fct.location_sk = loc.location_sk
    group by
        fct.channel
        , fct.location_sk
        , date_trunc(
            'month'
            , to_date(cast(fct.date_sk as string), 'yyyyMMdd')
        )
        , coalesce(loc.location_state, 'NÃO INFORMADO')
        , coalesce(loc.city, 'NÃO INFORMADO')
),

final_aggregation as (
    select
        -- Primary Key da Agregação
        {{ dbt_utils.generate_surrogate_key([
            'month_start_date'
            , 'channel'
            , 'location_sk'
        ]) }} as channel_monthly_sk

        -- Atributos de Agrupamento
        , month_start_date
        , channel
        , location_sk
        , location_state
        , city

        -- Métricas Financeiras
        , total_gross_revenue
        , total_discount_amount
        , total_gross_sales_amount
        , total_refund_amount
        , total_net_sales_amount
        , total_active_invoiced_amount

        -- Quantidades e Volume
        , total_quantity_sold
        , total_net_quantity_sold
        , total_orders_count

        -- Desconto Médio Percentual Praticado (%)
        , round(
            coalesce(
                (total_discount_amount / nullif(total_gross_revenue, 0)) * 100
                , 0
            )
            , 2
        ) as average_discount_percentage

        -- Metadata
        , current_timestamp() as aggregated_at
    from aggregated_sales
)

select
    channel_monthly_sk
    , month_start_date
    , channel
    , location_sk
    , location_state
    , city
    , total_gross_revenue
    , total_discount_amount
    , total_gross_sales_amount
    , total_refund_amount
    , total_net_sales_amount
    , total_active_invoiced_amount
    , total_quantity_sold
    , total_net_quantity_sold
    , total_orders_count
    , average_discount_percentage
    , aggregated_at
from final_aggregation
