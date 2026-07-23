-- =====================================================================
-- Model: fct_order_payments
-- Description: Fact table recording order payments, payment methods,
--              amounts, and foreign surrogate keys for dimensions.
-- =====================================================================

with payments as (
    select * from {{ ref('stg_lh_nautical_payments') }}
),

orders_summary as (
    -- Agrupamos para garantir 1 linha por order_id e evitar duplicar pagamentos
    select
        order_id
        , customer_id
        , is_cancelled
    from {{ ref('int_orders_with_items') }}
    group by
        order_id
        , customer_id
        , is_cancelled
),

joined_payments as (
    select
        pym.payment_id
        , pym.order_id
        , ord.customer_id
        , lower(trim(pym.payment_method)) as payment_method
        , cast(pym.payment_amount as decimal(14, 2)) as payment_amount
        , pym.paid_at
        , pym.ingestion_timestamp
    from payments as pym
    left join orders_summary as ord
        on pym.order_id = ord.order_id
    where coalesce(ord.is_cancelled, false) = false
),

calculated_sks as (
    select
        -- 1. Colunas puras primeiro (Regra ST06)
        payment_id
        , order_id
        , customer_id
        , payment_method
        , payment_amount
        , paid_at
        , ingestion_timestamp

        -- 2. Geração das Chaves para o Star Schema
        , {{ dbt_utils.generate_surrogate_key([
            'payment_id'
        ]) }} as payment_sk

        , case
            when customer_id is not null
                then {{ dbt_utils.generate_surrogate_key([
                    'customer_id'
                ]) }}
        end as customer_sk

        -- Date SK numérica YYYYMMDD com tratamento para data nula
        , coalesce(
            cast(date_format(paid_at, 'yyyyMMdd') as int)
            , -1
        ) as date_sk
    from joined_payments
)

-- 3. SELECT final organizado
select
    -- Primary & Foreign Keys
    payment_sk
    , date_sk
    , customer_sk

    -- Natural Keys & Degenerate Dimensions
    , payment_id
    , order_id
    , customer_id
    , payment_method

    -- Measures
    , payment_amount

    -- Dates & Metadata
    , paid_at
    , ingestion_timestamp
from calculated_sks