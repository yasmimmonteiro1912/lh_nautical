-- =====================================================================
-- Model: agg_inventory_monthly_health
-- Layer: Gold / Marts
-- Description: Monthly aggregate model for inventory health, turnover,
--              and idle stock analysis.
-- =====================================================================

with snapshots as (
    select * from {{ ref('fct_inventory_snapshot_daily') }}
),

movements as (
    select * from {{ ref('fct_inventory_movements') }}
),

-- CTE 1: Agregacao mensal do saldo diario de estoque e valor imobilizado
monthly_snapshots as (
    select
        variant_sk
        , location_sk
        , product_variant_id
        , location_id
        , cast(substr(cast(date_sk as string), 1, 6) as int)
            as year_month_key
        , round(avg(quantity_on_hand), 2) as avg_quantity_on_hand
        , round(avg(stock_value_estimated), 2) as avg_stock_value_estimated
    from snapshots
    group by
        variant_sk
        , location_sk
        , product_variant_id
        , location_id
        , cast(substr(cast(date_sk as string), 1, 6) as int)
),

-- CTE 2: Agregacao mensal do fluxo de entradas (IN) e saidas (OUT)
monthly_movements as (
    select
        variant_sk
        , location_sk
        , cast(substr(cast(date_sk as string), 1, 6) as int)
            as year_month_key
        , coalesce(
            sum(case when quantity > 0 then quantity else 0 end), 0
        ) as total_entries_quantity
        , coalesce(
            sum(case when quantity < 0 then abs(quantity) else 0 end), 0
        ) as total_exits_quantity
    from movements
    group by
        variant_sk
        , location_sk
        , cast(substr(cast(date_sk as string), 1, 6) as int)
),

-- CTE 3: Anchor nos snapshots (LEFT JOIN) para limitar aos meses processados
joined_health as (
    select
        -- 1. Chaves de Negocio do Grao (Baseadas no Snapshot)
        snap.year_month_key
        , snap.variant_sk
        , snap.location_sk
        , snap.product_variant_id
        , snap.location_id

        -- 2. Metricas de Saude e Giro de Estoque
        , snap.avg_quantity_on_hand
        , snap.avg_stock_value_estimated
        , coalesce(mov.total_entries_quantity, 0) as total_entries_quantity
        , coalesce(mov.total_exits_quantity, 0) as total_exits_quantity

    from monthly_snapshots as snap
    left join monthly_movements as mov
        on
            snap.year_month_key = mov.year_month_key
            and snap.variant_sk = mov.variant_sk
            and snap.location_sk = mov.location_sk
),

final_model as (
    select
        -- 1. Colunas simples primeiro (Regra ST06 do SQLFluff)
        year_month_key
        , variant_sk
        , product_variant_id
        , location_sk
        , location_id
        , avg_quantity_on_hand
        , avg_stock_value_estimated
        , total_entries_quantity
        , total_exits_quantity

        -- 2. Primary Surrogate Key da Tabela Agregada
        , {{ dbt_utils.generate_surrogate_key([
            'year_month_key',
            'variant_sk',
            'location_sk'
        ]) }} as inventory_health_sk

        -- 3. Timestamp de Auditoria
        , current_timestamp() as ingestion_timestamp
    from joined_health
)

-- SELECT final padronizado para consumo no Power BI
select
    inventory_health_sk
    , year_month_key
    , variant_sk
    , product_variant_id
    , location_sk
    , location_id
    , avg_quantity_on_hand
    , avg_stock_value_estimated
    , total_entries_quantity
    , total_exits_quantity
    , ingestion_timestamp
from final_model
