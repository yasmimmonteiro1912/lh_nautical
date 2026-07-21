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
        -- 1. Chave Primária da Fato (Surrogate Key)
        md5(cast(mov.stock_movement_id as string)) as movement_sk
        
        -- 2. Chaves Estrangeiras (FKs)
        -- Converte o timestamp occurred_at para inteiro AAAAMMDD (Ex: 20260721)
        , cast(date_format(mov.occurred_at, 'yyyyMMdd') as int) as date_sk
        , md5(cast(mov.product_variant_id as string)) as variant_sk
        , md5(cast(mov.location_id as string)) as location_sk
        
        -- Chave composta conectando com a dim_movement_type da Yasmim
        , {{ dbt_utils.generate_surrogate_key([
            'mov.movement_type',
            'mov.reference_table'
        ]) }} as movement_type_sk
        
        -- FK do funcionário (trata nulo caso seja uma movimentação automática de sistema)
        , case
            when mov.employee_id is not null
                then md5(cast(mov.employee_id as string))
        end as employee_sk
        
        -- 3. Atributos Degenerados
        , mov.reference_id
        -- Pegando a coluna higienizada da staging (movement_notes) e entregando como notes
        , mov.movement_notes as notes
        
        -- 4. Métrica de Fluxo de Estoque
        -- Regra: Como a staging entrega 'out' em minúsculas, invertemos o sinal para negativo
        , case
            when lower(trim(mov.movement_type)) like '%out%'
                then -1 * abs(mov.quantity)
            else abs(mov.quantity)
          end as quantity

    from stg_movements as mov
)

select
    movement_sk
    , date_sk
    , variant_sk
    , location_sk
    , movement_type_sk
    , employee_sk
    , reference_id
    , quantity
    , notes
from transformed_movements
