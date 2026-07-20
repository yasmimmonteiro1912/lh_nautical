with enriched_customers as (
    select * from {{ ref('int_customers_enriched') }}
),

final_dimension as (
    select
        -- Chave substituta (Surrogate Key) para os joins do Star Schema
        cst.customer_id
        , cst.person_type
        , cst.legal_name
        , cst.trade_name
        , cst.tax_id
        , cst.tax_id_type
        , cst.state_registration
        , cst.state_tax_status
        , cst.customer_email
        , cst.customer_phone
        , cst.is_active
        , cst.primary_postal_code
        
        -- Endereço desnormalizado pronto para os relatórios
        , cst.primary_street
        , cst.primary_street_number
        , cst.primary_district
        , cst.primary_city
        , cst.primary_state
        , cst.primary_country
        , cst.ingestion_timestamp
        
        -- Timestamp de auditoria
        , md5(cast(cst.customer_id as string)) as customer_sk
    from enriched_customers as cst
)

select * from final_dimension
