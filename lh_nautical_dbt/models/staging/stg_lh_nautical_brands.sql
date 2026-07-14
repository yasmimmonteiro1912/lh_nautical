with source_brands as (

    select * 
    from {{ source('lh_nautical', 'brands') }}

),

renamed as (

    select
        cast(id as int) as brand_id
        , cast(name as string) as brand_name
        , cast(country as string) as brand_country -- Aceita os valores nulos [NULL] vindos da origem
        , cast(is_active as boolean) as is_active -- Transforma o tipo bit (0/1) em Boolean (true/false)
    from source_brands

)

select * 
from renamed
