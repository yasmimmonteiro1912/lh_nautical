with seed_data as (
    select * from {{ ref('seed_movement_types') }}
),

calculated_sk as (
    select
        -- 1. Colunas simples primeiro
        mov.movement_type
        , mov.reference_table
        , mov.movement_category
        
        -- 2. Geração da SK via macro dbt_utils
        , {{ dbt_utils.generate_surrogate_key([
            'mov.movement_type',
            'mov.reference_table'
        ]) }} as movement_type_sk
    from seed_data as mov
)

-- 3. SELECT final com a SK no topo para o Star Schema
select
    movement_type_sk
    , movement_type
    , reference_table
    , movement_category
from calculated_sk