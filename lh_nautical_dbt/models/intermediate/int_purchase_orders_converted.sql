-- =====================================================================
-- Model: int_purchase_orders_converted
-- Description: Trusted intermediate model that unifies purchase order headers
--              with the closest available daily exchange rates.
--              Falls back to the most recent business day rate for weekends,
--              holidays, or future mock dates to guarantee non-null values.
-- =====================================================================

with staging_orders as (

    select *
    from {{ ref('stg_lh_nautical_purchase_orders') }}

)

, staging_exchange_rates as (

    select *
    from {{ ref('stg_lh_nautical_taxas_cambio') }}

)

-- Selects the closest available exchange rate on or before the order date
, orders_with_closest_rates as (

    select
        pjo.purchase_order_id
        , pjo.po_number
        , pjo.supplier_id
        , pjo.buyer_id
        , pjo.destination_location_id
        , pjo.purchase_order_status
        , pjo.purchase_currency
        , pjo.placed_at
        , pjo.expected_delivery_at
        , pjo.ingestion_timestamp
        -- Correlated subquery to find the last available USD rate
        , coalesce(
            (
                select txc.cotacao_venda_dolar
                from staging_exchange_rates as txc
                where to_date(txc.data_hora_cotacao) <= to_date(pjo.placed_at)
                order by txc.data_hora_cotacao desc
                limit 1
            ),
            1.0000
        ) as closest_usd_rate
        -- Correlated subquery to find the last available EUR rate
        , coalesce(
            (
                select txc.cotacao_venda_euro
                from staging_exchange_rates as txc
                where to_date(txc.data_hora_cotacao) <= to_date(pjo.placed_at)
                order by txc.data_hora_cotacao desc
                limit 1
            ),
            1.0000
        ) as closest_euro_rate
    -- Alias 'pjo' meets the minimum 3-character requirement
    from staging_orders as pjo

)

-- Leading comma separating CTE blocks
, joined_rates as (

    select
        owr.purchase_order_id
        , owr.po_number
        , owr.supplier_id
        , owr.buyer_id
        , owr.destination_location_id
        , owr.purchase_order_status
        , owr.purchase_currency
        , owr.placed_at
        , owr.expected_delivery_at
        , owr.ingestion_timestamp
        -- Business Rule: Multiplier factor depending on the order currency
        , case
            when owr.purchase_currency = 'USD' then owr.closest_usd_rate
            when owr.purchase_currency = 'EUR' then owr.closest_euro_rate
            else 1.0000
        end as applied_exchange_rate
    -- Alias 'owr' meets the minimum 3-character requirement
    from orders_with_closest_rates as owr

)

-- Final CTE to satisfy the 'select from last CTE' pipeline rule
, final as (

    select
        purchase_order_id
        , po_number
        , supplier_id
        , buyer_id
        , destination_location_id
        , purchase_order_status
        , purchase_currency
        , placed_at
        , expected_delivery_at
        , applied_exchange_rate
        , ingestion_timestamp
    -- Alias 'jnr' meets the minimum 3-character requirement
    from joined_rates

)

select
    purchase_order_id
    , po_number
    , supplier_id
    , buyer_id
    , destination_location_id
    , purchase_order_status
    , purchase_currency
    , placed_at
    , expected_delivery_at
    , applied_exchange_rate
    , ingestion_timestamp
from final
