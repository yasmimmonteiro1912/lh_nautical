with source_categories as (

    select * 
    from {{ source('lh_nautical', 'categories') }}

),

renamed as (

    select
        cast(id as int) as category_id
        , cast(name as varchar) as category_name
        , cast(slug as varchar) as category_slug
        , cast(parent_category_id as int) as parent_category_id -- Preserva o relacionamento para a hierarquia de subcategorias
        , cast(is_active as boolean) as is_active -- Converte o tipo bit (0/1) da origem para lógico
    from source_categories

)

select * 
from renamed