-- =====================================================================
-- Model: fct_inventory_movements
-- Description: Fact table recording physical stock fluctuations
--              (entries, exits, adjustments) in warehouses and stores.
-- =====================================================================

with int_movements as (
    select * from {{ ref('int_stock_movements_sanitized') }}
),

transformed_movements as (
    select
        -- 1. Chaves Naturais e Colunas Puras
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
        , mov.ingestion_timestamp

        -- 2. Métricas
        , mov.audited_quantity as quantity

        -- 3. Chave de Data (AAAAMMDD)
        , cast(date_format(mov.occurred_at, 'yyyyMMdd') as int) as date_sk

        -- 4. Surrogate Keys padronizadas via dbt_utils
        , {{ dbt_utils.generate_surrogate_key(['mov.stock_movement_id']) }}
            as movement_sk

        , {{ dbt_utils.generate_surrogate_key(['mov.product_variant_id']) }}
            as variant_sk

        , {{ dbt_utils.generate_surrogate_key(['mov.location_id']) }}
            as location_sk

        , {{ dbt_utils.generate_surrogate_key([
            'mov.movement_type',
            'mov.reference_table'
        ]) }} as movement_type_sk

        -- SK do Colaborador (Trata nulos para movimentações do sistema/saldo inicial)
        , case
            when mov.employee_id is not null
                then {{ dbt_utils.generate_surrogate_key(['mov.employee_id']) }}
        end as employee_sk

    from int_movements as mov
)

select
    -- Primary Key
    movement_sk

    -- Foreign Keys (Star Schema Dimensions)
    , date_sk
    , variant_sk
    , location_sk
    , movement_type_sk
    , employee_sk

    -- Natural Keys / Degenerate Dimensions (Para Auditoria/Debugging)
    , stock_movement_id
    , product_variant_id
    , location_id
    , employee_id
    , reference_id
    , reference_table
    , movement_type
    , notes
    , is_pending_review
    , is_audited

    -- Measures
    , quantity

    -- Metadata
    , ingestion_timestamp
from transformed_movements