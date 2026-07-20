with staging_customers as (
    select * from {{ ref('stg_lh_nautical_customers') }}
),

tax_classification as (
    select
        customer_id
        , person_type
        , legal_name
        , trade_name
        , tax_id
        , state_registration
        , customer_email
        , customer_phone
        , is_active
        , ingestion_timestamp
        -- Lógica de classificação fiscal mantida
        , case
            when length(trim(tax_id)) = 11 then 'CPF'
            when length(trim(tax_id)) = 14 then 'CNPJ'
            else 'INVALID_TAX_ID'
        end as tax_id_type
        , case
            when state_registration is null then 'Non-Contributor'
            when upper(trim(state_registration)) = 'ISENTO' then 'Exempt'
            else 'Contributor'
        end as state_tax_status
    from staging_customers
),

primary_addresses as (
    -- Filtra apenas o endereço principal para evitar duplicar linhas do cliente
    select * 
    from {{ ref('stg_lh_nautical_addresses') }}
    where is_primary = true
),

final_joined as (
    select
        tax.customer_id
        , tax.person_type
        , tax.legal_name
        , tax.trade_name
        , tax.tax_id
        , tax.tax_id_type
        , tax.state_registration
        , tax.state_tax_status
        , tax.customer_email
        , tax.customer_phone
        , tax.is_active
        -- Dados de endereço desnormalizados vindos da staging de endereços
        , adr.postal_code as primary_postal_code
        , adr.street as primary_street
        , adr.street_number as primary_street_number
        , adr.district as primary_district
        , adr.city as primary_city
        , adr.customer_state as primary_state
        , adr.country as primary_country
        -- Garante o tracking do timestamp de auditoria mais recente
        , case
            when adr.ingestion_timestamp > tax.ingestion_timestamp
                then adr.ingestion_timestamp
            else tax.ingestion_timestamp
        end as ingestion_timestamp
    from tax_classification as tax
    left join primary_addresses as adr 
        on tax.customer_id = adr.customer_id
)

select * from final_joined
