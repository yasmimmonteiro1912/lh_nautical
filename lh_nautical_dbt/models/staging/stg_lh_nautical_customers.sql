with source_customers as (

    select * 
    from {{ source('lh_nautical', 'customers') }}

),

renamed as (

    select
        cast(id as int) as customer_id
        , cast(person_type as varchar) as person_type -- Identifica se é PF ou PJ
        , cast(legal_name as varchar) as legal_name
        , cast(trade_name as varchar) as trade_name
        , cast(tax_id as varchar) as tax_id -- CPF ou CNPJ
        , cast(state_registration as varchar) as state_registration
        , cast(email as varchar) as customer_email
        , cast(phone as varchar) as customer_phone
        , cast(is_active as boolean) as is_active -- Converte o tipo bit (0/1) da origem para lógico
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
    from source_customers

)

select * 
from renamed