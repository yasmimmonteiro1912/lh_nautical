-- =====================================================================
-- Model: stg_lh_nautical_stock_movements
-- Description: Cleans, standardizes, and formats stock movement logs.
--              - Converts lazy placeholders ('...', '—') to NULL.
--              - Safely coerces empty strings to NULL.
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'stock_movements') }} -- noqa: LT01

)

-- Leading comma separating the CTE blocks to prevent the PARSE_SYNTAX_ERROR
, cleaned_records as (

    select
        cast(id as bigint) as stock_movement_id
        , cast(product_variant_id as int) as product_variant_id
        , cast(location_id as int) as location_id
        , cast(employee_id as int) as employee_id
        , cast(reference_id as int) as reference_id
        
        -- Transforma string vazia em NULL de forma segura
        , cast(quantity as decimal(14,3)) as quantity
        , cast(occurred_at as timestamp) as occurred_at
        
        -- Limpa placeholders identificados no DBeaver ('...', '—')
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
        
        , lower(trim(cast(movement_type as string))) as movement_type
        , nullif(lower(trim(cast(reference_table as string))), '')
            as reference_table
        , case
            when trim(cast(notes as string)) in ('...', '—', '-') then null
            
            -- Consolida sinônimos de sucesso/verificação
            when
                lower(trim(cast(notes as string))) in (
                    'ok', 'ok!', 'tudo certo', 'conferido'
                )
                then 'conferido'
            
            -- Consolida sinônimos de pendência/atenção
            when
                lower(trim(cast(notes as string))) in (
                    'checar', 'ver com gerente'
                )
                then 'necessita_revisao'
            
            -- Padroniza registro de saldo inicial/migração
            when
                lower(trim(cast(notes as string))) like 'opening balance%'
                then 'saldo_inicial'
            
            -- Preserva notas customizadas não enquadradas acima
            else nullif(trim(cast(notes as string)), '')
        end as movement_notes
    from source_data

)

select
    stock_movement_id
    , product_variant_id
    , location_id
    , employee_id
    , movement_type
    , reference_table
    , reference_id
    , movement_notes
    , quantity
    , occurred_at
    , ingestion_timestamp
from cleaned_records
