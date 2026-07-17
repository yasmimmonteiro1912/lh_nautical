with source_attributes as (

    select * 
    from {{ source('lh_nautical', 'attributes') }}

),

renamed as (

    select
        cast(id as int) as attribute_id
        , cast(name as string) as attribute_name
        , cast(data_type as string) as data_type
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_attributes

)

select * 
from renamed
