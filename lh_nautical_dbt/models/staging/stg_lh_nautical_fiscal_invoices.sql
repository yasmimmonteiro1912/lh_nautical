with source_fiscal_invoices as (

    select * 
    from {{ source('lh_nautical', 'fiscal_invoices') }}

),

renamed as (

    select
        cast(id as int) as fiscal_invoice_id
        , cast(order_id as int) as order_id
        , cast(nfe_number as varchar) as nfe_number
        , cast(nfe_access_key as varchar) as nfe_access_key
        , cast(series as varchar) as nfe_series
        , cast(issued_at as timestamp) as issued_at
        , cast(status as varchar) as invoice_status
        , cast(total_amount as decimal(9,2)) as total_amount
        , cast(xml_storage_uri as varchar) as xml_storage_uri
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
    from source_fiscal_invoices

)

select * 
from renamed