with source_fiscal_invoices as (

    select * 
    from {{ source('lh_nautical', 'fiscal_invoices') }}

),

renamed as (

    select
        cast(id as int) as fiscal_invoice_id
        , cast(order_id as int) as order_id
        , cast(nfe_number as string) as nfe_number
        , cast(nfe_access_key as string) as nfe_access_key
        , cast(series as string) as nfe_series
        , cast(issued_at as timestamp) as issued_at
        , cast(status as string) as invoice_status
        , cast(total_amount as decimal(9,2)) as total_amount
        , cast(xml_storage_uri as string) as xml_storage_uri
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_fiscal_invoices

)

select * 
from renamed
