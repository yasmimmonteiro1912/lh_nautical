-- =====================================================================
-- Model: stg_lh_nautical_returns
-- Description: Cleans, standardizes, and formats return headers.
--              - Targeted semantic mapping: consolidates 26 
--                typo-ridden reasons into 7 clean audit categories.
--              - Safely coerces empty strings and garbage to NULL.
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'returns') }} -- noqa: LT01

)

-- Leading comma separating the CTE blocks to prevent the PARSE_SYNTAX_ERROR
, cleaned_records as (

    select
        cast(id as int) as return_id
        , cast(customer_id as int) as customer_id
        , cast(order_id as int) as order_id
        , cast(received_at_location_id as int) as received_at_location_id
        , cast(created_at as timestamp) as returned_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
        
        -- Consolidação de motivos de devolução (Tratamento de typos)
        , trim(cast(return_number as string)) as return_number
          
        , lower(trim(cast(status as string))) as return_status
        , case
            -- 1. Remove lixo operacional
            when trim(cast(reason as string)) in ('—', '?', '-') then null
            
            -- 2. Agrupa desistências (trata desistiu e desistui)
            when lower(trim(cast(reason as string))) like 'cliente desist%' 
                then 'cliente_desistiu'
            
            -- 3. Agrupa compras duplicadas
            when lower(trim(cast(reason as string))) like 'compra duplicada%' 
                then 'compra_duplicada'
            
            -- 4. Agrupa divergências de descrição 
            -- (tratando typos como 'corressponde' e 'descriçãooo')
            when lower(trim(cast(reason as string))) like 'item n%' 
                then 'item_divergente_da_descricao'
            
            -- 5. Agrupa problemas de transporte e avarias 
            -- (tratando 'variado' e 'oavariado')
            when
                lower(trim(cast(reason as string))) like '%avariado%' 
                or lower(trim(cast(reason as string))) like 'produto variado%' 
                then 'produto_avariado_transporte'
              
            -- 6. Agrupa tamanhos e cores erradas 
            -- (tratando 'coor', 'iincorrectos' e 'incorretoos')
            when lower(trim(cast(reason as string))) like 'tamanho/%' 
                then 'tamanho_cor_incorreto'
            
            -- 7. Agrupa defeitos de fábrica 
            -- (tratando 'defeitoo', 'ccom' e espaços duplos)
            when lower(trim(cast(reason as string))) like '%defeito%' 
                then 'defeito_de_fabrica'
            
            -- 8. Categoria outros
            when lower(trim(cast(reason as string))) = 'outros' 
                then 'outros'
            
            -- 9. Se aparecer algo diferente, preserva em minúsculo
            else nullif(lower(trim(cast(reason as string))), '')
        end as return_reason
    from source_data

)

select
    return_id
    , return_number
    , customer_id
    , order_id
    , received_at_location_id
    , return_status
    , return_reason
    , returned_at
    , ingestion_timestamp
from cleaned_records
