-- =====================================================================
-- Model: fct_inventory_movements
-- Description: Fact table recording physical stock fluctuations
--              (entries, exits, adjustments) in warehouses and stores.
-- =====================================================================

with stg_movements as (
    select * from {{ ref('stg_lh_nautical_stock_movements') }}
),

transformed_movements as (
    select
        mov.stock_movement_id
        , mov.product_variant_id
        , mov.location_id
        , mov.employee_id
        , mov.reference_id
        , mov.movement_notes as notes
        , mov.ingestion_timestamp

        , cast(date_format(mov.occurred_at, 'yyyyMMdd') as int) as date_sk

        -- 3. Surrogate Keys padronizadas via dbt_utils
        , {{ dbt_utils.generate_surrogate_key(['mov.stock_movement_id']) }}
            as movement_sk
        , {{ dbt_utils.generate_surrogate_key(['mov.product_variant_id']) }}
            as variant_sk
        , {{ dbt_utils.generate_surrogate_key(['mov.location_id']) }}
            as location_sk

        -- Chave composta do Tipo de Movimentacao
        , {{ dbt_utils.generate_surrogate_key([
            'mov.movement_type',
            'mov.reference_table'
        ]) }} as movement_type_sk

        -- SK do Colaborador (Trata nulos para movimentacoes do sistema)
        , case
            when mov.employee_id is not null
                then {{ dbt_utils.generate_surrogate_key(['mov.employee_id']) }}
        end as employee_sk

        , case
            when lower(trim(mov.movement_type)) like '%out%'
                then -1 * abs(mov.quantity)
            else abs(mov.quantity)
        end as quantity

    from stg_movements as mov
)

select
    movement_sk
    , stock_movement_id
    , date_sk
    , variant_sk
    , product_variant_id
    , location_sk
    , location_id
    , movement_type_sk
    , employee_sk
    , employee_id
    , reference_id
    , quantity
    , notes
    , ingestion_timestamp
from transformed_movements
