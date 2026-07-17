with source_customers as (

    select * 
    from {{ source('lh_nautical', 'customers') }}

),

renamed as (

    select
        cast(id as int) as customer_id
        -- Identifica se é PF ou PJ
        , cast(person_type as string) as person_type 
        , cast(legal_name as string) as legal_name
        , cast(trade_name as string) as trade_name
        -- CPF ou CNPJ
        , cast(tax_id as string) as tax_id 
        , cast(state_registration as string) as state_registration
        , cast(email as string) as customer_email
        , cast(phone as string) as customer_phone
        -- Converte o tipo bit (0/1) da origem para lógico
        , cast(is_active as boolean) as is_active 
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_customers

)

select * 
from renamed
