with enriched_customers as (
    select * from {{ ref('int_customers_enriched') }}
),

calculated_fields as (
    select
        -- 1. Colunas simples/diretas primeiro (Atende a regra ST06 do SQLFluff)
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
        , cst.primary_street
        , cst.primary_street_number
        , cst.primary_district
        , cst.primary_city
        , cst.primary_state
        , cst.primary_country
        , cst.ingestion_timestamp

        -- 2. Geração da SK via macro dbt_utils
        , {{ dbt_utils.generate_surrogate_key(['cst.customer_id']) }}
            as customer_sk
    from enriched_customers as cst
)

-- 3. SELECT final organizando a chave SK no topo para o Star Schema
select
    customer_sk
    , customer_id
    , person_type
    , legal_name
    , trade_name
    , tax_id
    , tax_id_type
    , state_registration
    , state_tax_status
    , customer_email
    , customer_phone
    , is_active
    , primary_postal_code
    , primary_street
    , primary_street_number
    , primary_district
    , primary_city
    , primary_state
    , primary_country
    , ingestion_timestamp
from calculated_fields
