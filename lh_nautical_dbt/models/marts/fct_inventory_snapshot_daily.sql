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
    -- [MELHORIA 1]: Puxando da Intermediate int_product_variants_core
    select * from {{ ref('int_product_variants_core') }}
),

calculated_snapshot as (
    select
        sl.product_variant_id
        , sl.location_id
        , sl.quantity_on_hand
        , sl.ingestion_timestamp

        , cast(date_format(current_date(), 'yyyyMMdd') as int) as date_sk
        , cast(
            sl.quantity_on_hand * coalesce(pv.cost_price, 0)
            as decimal(18, 2)
        ) as stock_value_estimated

        , {{ dbt_utils.generate_surrogate_key(['sl.product_variant_id']) }}
            as variant_sk
        , {{ dbt_utils.generate_surrogate_key(['sl.location_id']) }}
            as location_sk
    from stock_levels as sl
    left join product_variants as pv
        on sl.product_variant_id = pv.product_variant_id
),

final_snapshot as (
    select
        -- Colunas simples primeiro
        product_variant_id
        , location_id
        , date_sk
        , variant_sk
        , location_sk
        , quantity_on_hand
        , stock_value_estimated
        , ingestion_timestamp

        , {{ dbt_utils.generate_surrogate_key([
            'date_sk',
            'location_id',
            'product_variant_id'
        ]) }} as snapshot_sk
    from calculated_snapshot
)

select
    snapshot_sk
    , date_sk
    , variant_sk
    , product_variant_id
    , location_sk
    , location_id
    , quantity_on_hand
    , stock_value_estimated
    , ingestion_timestamp
from final_snapshot
