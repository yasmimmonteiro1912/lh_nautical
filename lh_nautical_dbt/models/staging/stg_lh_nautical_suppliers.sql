with source_data as (

    select *
    from {{ source('lh_nautical', 'suppliers') }} -- noqa: LT01

)

, renamed as (

    select
        id as supplier_id
        , legal_name
        , trade_name
        , tax_id
        , tax_id_type
        , contact_name
        , email
        , phone
        , country
        , is_active as supplier_is_active
        , ingestion_timestamp
    from source_data

)

select *
from renamed
