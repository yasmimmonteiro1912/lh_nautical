-- =====================================================================
-- Model: fct_inventory_snapshot_daily
-- Materialization: INCREMENTAL
-- =====================================================================

{{ config(
    materialized='incremental',
    unique_key='snapshot_sk'
) }}

with stock_levels as (
    select * from {{ ref('stg_lh_nautical_stock_levels') }}
),

product_variants as (
    select * from {{ ref('stg_lh_nautical_product_variants') }}
),

calculated_snapshot as (
    select
        -- 1. Colunas simples primeiro (Atende a regra ST06 do SQLFluff)
        sl.quantity_on_hand

        -- 2. Colunas calculadas / expressoes
        , cast(date_format(current_date(), 'yyyyMMdd') as int) as date_sk
        , cast(
            sl.quantity_on_hand * coalesce(pv.cost_price, 0)
            as decimal(18, 2)
        ) as stock_value_estimated
        , md5(cast(sl.product_variant_id as string)) as variant_sk
        , md5(cast(sl.location_id as string)) as location_sk
    from stock_levels as sl
    left join product_variants as pv
        on sl.product_variant_id = pv.product_variant_id
),

final_snapshot as (
    select
        -- 1. Colunas simples primeiro
        date_sk
        , variant_sk
        , location_sk
        , quantity_on_hand
        , stock_value_estimated

        -- 2. Macro / Calculo por ultimo
        , {{ dbt_utils.generate_surrogate_key([
            'date_sk',
            'location_sk',
            'variant_sk'
        ]) }} as snapshot_sk
    from calculated_snapshot
)

-- SELECT final padronizado na ordem da Fato (Star Schema)
select
    snapshot_sk
    , date_sk
    , variant_sk
    , location_sk
    , quantity_on_hand
    , stock_value_estimated
from final_snapshot
