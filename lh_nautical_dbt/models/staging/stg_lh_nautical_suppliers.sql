with source_data as (

    select *
    from {{ source('lh_nautical', 'suppliers') }} -- noqa: LT01

)

, renamed as (

    select
        cast(id as int) as supplier_id
        , cast(legal_name as string) as legal_name
        , cast(trade_name as string) as trade_name
        , cast(tax_id as string) as tax_id
        , cast(tax_id_type as string) as tax_id_type
        , cast(contact_name as string) as contact_name
        , cast(email as string) as email
        , cast(phone as string) as phone
        , cast(country as string) as country
        , cast(is_active as boolean) as supplier_is_active
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
