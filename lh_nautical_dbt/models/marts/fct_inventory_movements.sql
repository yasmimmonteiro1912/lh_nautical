-- =====================================================================
-- Model: fct_inventory_movements
-- Description: Fact table recording physical stock fluctuations
--              (purchases, sales, customer returns, vendor returns).
-- =====================================================================

with int_movements as (
    select * from {{ ref('int_stock_movements_sanitized') }}
),

returns_summary as (
    select
        return_id
        , customer_id
        , return_reason
    from {{ ref('int_returns') }}
),

transformed_movements as (
    select
        -- 1. Chaves Naturais e Colunas Puras (Regra ST06)
        mov.stock_movement_id
        , mov.product_variant_id
        , mov.location_id
        , mov.employee_id
        , mov.reference_id
        , mov.reference_table
        , mov.movement_type
        , mov.movement_note_status as notes
        , mov.is_pending_review
        , mov.is_audited
        , ret.customer_id as return_customer_id
        , ret.return_reason
        , mov.ingestion_timestamp

        -- 2. Métricas de Quantidade
        , mov.audited_quantity as quantity

        -- 3. Chave de Data (YYYYMMDD com fallback -1 para nulos)
        , coalesce(
            cast(date_format(mov.occurred_at, 'yyyyMMdd') as int)
            , -1
        ) as date_sk

        -- 4. Surrogate Keys para Star Schema via dbt_utils
        , {{ dbt_utils.generate_surrogate_key([
            'mov.stock_movement_id'
        ]) }} as movement_sk

        , {{ dbt_utils.generate_surrogate_key([
            'mov.product_variant_id'
        ]) }} as variant_sk

        , {{ dbt_utils.generate_surrogate_key([
            'mov.location_id'
        ]) }} as location_sk

        , {{ dbt_utils.generate_surrogate_key([
            'mov.movement_type'
            , 'mov.reference_table'
        ]) }} as movement_type_sk

        -- SK do Colaborador (Nulo em movimentações automáticas de sistema)
        , case
            when mov.employee_id is not null
                then {{ dbt_utils.generate_surrogate_key([
                    'mov.employee_id'
                ]) }}
        end as employee_sk

        -- SK do Cliente (Preenchida apenas em devoluções)
        , case
            when ret.customer_id is not null
                then {{ dbt_utils.generate_surrogate_key([
                    'ret.customer_id'
                ]) }}
        end as customer_sk

    from int_movements as mov
    left join returns_summary as ret
        on mov.reference_id = ret.return_id
        and mov.movement_type in ('return_in', 'return_out')
)

select
    -- Primary & Foreign Keys
    movement_sk
    , date_sk
    , variant_sk
    , location_sk
    , movement_type_sk
    , employee_sk
    , customer_sk

    -- Natural Keys / Degenerate Dimensions
    , stock_movement_id
    , product_variant_id
    , location_id
    , employee_id
    , return_customer_id
    , reference_id
    , reference_table
    , movement_type
    , return_reason
    , notes
    , is_pending_review
    , is_audited

    -- Measures
    , quantity

    -- Metadata
    , ingestion_timestamp
from transformed_movements