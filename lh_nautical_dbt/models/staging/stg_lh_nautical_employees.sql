with source_data as (

    select * 
    from {{ source('lh_nautical', 'employees') }}

),

renamed as (

    select
        cast(id as int) as employee_id
        , cast(full_name as string) as employee_full_name
        , cast(cpf as string) as employee_cpf
        , cast(email as string) as employee_email
        , cast(role as string) as employee_role
        , cast(primary_location_id as int) as primary_location_id
        , cast(hire_date as date) as hire_date
        , cast(termination_date as date) as termination_date
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select * 
from renamed
