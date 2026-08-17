-- =====================================================================
-- Model: agg_inventory_idle_stock
-- Layer: Gold / Marts
-- Description: Point-in-time model identifying idle stock (SKUs without
--              a sale for N+ days) and the capital value tied up in them.
-- =====================================================================

with movements as (
    select * from {{ ref('fct_inventory_movements') }}
),

movement_types as (
    select * from {{ ref('dim_movement_type') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
),

snapshots as (
    select * from {{ ref('fct_inventory_snapshot_daily') }}
),

-- CTE 1: Movimentos de venda com data real (join com dim_dates e dim_movement_type)
sales_movements as (
    select
        mov.variant_sk
        , mov.location_sk
        , dt.full_date as sale_date
    from movements as mov
    inner join movement_types as mt
        on mov.movement_type_sk = mt.movement_type_sk
    inner join dates as dt
        on mov.date_sk = dt.date_sk
    where mt.movement_category = 'Venda'
),

-- CTE 2: Ultima data de venda por produto/local
last_sale as (
    select
        variant_sk
        , location_sk
        , max(sale_date) as last_sale_date
    from sales_movements
    group by variant_sk, location_sk
),

-- CTE 3: Data de referencia global (ultima venda registrada em toda a base)
reference_date as (
    select max(sale_date) as reference_date
    from sales_movements
),

-- CTE 4: Snapshot mais recente de estoque por produto/local
latest_snapshot as (
    select
        variant_sk
        , location_sk
        , product_variant_id
        , location_id
        , quantity_on_hand
        , stock_value_estimated
        , row_number() over (
            partition by variant_sk, location_sk
            order by date_sk desc
        ) as rn
    from snapshots
),

-- CTE 5: Junta estoque atual com ultima venda e calcula dias parado
joined_health as (
    select
        snap.variant_sk
        , snap.location_sk
        , snap.product_variant_id
        , snap.location_id
        , snap.quantity_on_hand as current_quantity_on_hand
        , snap.stock_value_estimated as current_stock_value_estimated
        , ls.last_sale_date
        , ref.reference_date
        , datediff(ref.reference_date, ls.last_sale_date) as days_since_last_sale
    from latest_snapshot as snap
    left join last_sale as ls
        on snap.variant_sk = ls.variant_sk
        and snap.location_sk = ls.location_sk
    cross join reference_date as ref
    where snap.rn = 1
),

final_model as (
    select
        -- 1. Colunas simples primeiro
        variant_sk
        , location_sk
        , product_variant_id
        , location_id
        , current_quantity_on_hand
        , current_stock_value_estimated
        , last_sale_date
        , reference_date
        , days_since_last_sale
        , last_sale_date is null as is_never_sold
        , coalesce(days_since_last_sale > 90, true) as is_idle_90d
        , case
            when coalesce(days_since_last_sale > 90, true)
                then current_stock_value_estimated
            else 0
        end as idle_capital_amount

        -- 2. Primary Surrogate Key da Tabela
        , {{ dbt_utils.generate_surrogate_key([
            'variant_sk',
            'location_sk'
        ]) }} as inventory_idle_sk

        -- 3. Timestamp de Auditoria
        , current_timestamp() as ingestion_timestamp
    from joined_health
)

select * from final_model