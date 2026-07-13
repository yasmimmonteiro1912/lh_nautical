with source_attributes as (

    select * 
    from {{ source('lh_nautical', 'attributes') }}

),

renamed as (

    select
        cast(id as int) as attribute_id
        , cast(name as varchar) as attribute_name
        , cast(data_type as varchar) as data_type
    from source_attributes

)

select * 
from renamed