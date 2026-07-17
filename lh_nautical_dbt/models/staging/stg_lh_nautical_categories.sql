with source_categories as (

    select * 
    from {{ source('lh_nautical', 'categories') }}

),

renamed as (

    select
        cast(id as int) as category_id
        , cast(name as string) as category_name
        , cast(slug as string) as category_slug
        -- Preserva o relacionamento para a hierarquia de categorias
        , cast(parent_category_id as int) as parent_category_id  
        -- Converte o tipo bit (0/1) da origem para lógico
        , cast(is_active as boolean) as is_active 
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_categories

)

select * 
from renamed
