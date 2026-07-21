with source_data as (
    select *
    from {{ source('lh_nautical', 'product_suppliers') }}
),

renamed as (
    select
        -- 1. Conversões simples de tipo primeiro (Atende a regra ST06 do SQLFluff)
        cast(product_variant_id as int) as product_variant_id
        , cast(supplier_id as int) as supplier_id
        , cast(last_quoted_cost as decimal(14, 2)) as last_quoted_cost
        , cast(lead_time_days as int) as lead_time_days
        , cast(is_preferred as boolean) as is_preferred
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        
        -- 2. Higienização e padronização do SKU do fornecedor (Transformação complexa)
        , case
            when trim(coalesce(supplier_sku, '')) = ''
                then null
            when lower(trim(supplier_sku)) in (
                '-'
                , '--'
                , '—'
                , '?'
                , 'sem-codigo'
                , 'sem código'
                , 'tbd'
                , 'todo'
                , 'nao informado'
                , 'não informado'
                , 'null'
                , 'none'
            )
                then null
            else upper(trim(supplier_sku))
        end as supplier_sku
        
        -- 3. Timestamp de auditoria por último
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data
)

select *
from renamed