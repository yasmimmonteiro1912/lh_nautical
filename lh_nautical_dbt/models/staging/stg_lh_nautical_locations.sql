with source_data as (

    select *
    from {{ source('lh_nautical', 'locations') }}

)

, renamed as (

    select
        cast(id as int) as location_id
        , cast(name as string) as location_name
        , cast(location_type as string) as location_type

        -- PADRONIZAÇÃO DO CEP (Apenas números e exatamente 8 dígitos)
        -- Ex: '50.012-345', '50 012 345' ou '50012-345' -> '50012345'
        -- Nota: Mantemos sem hífen no Staging/Intermediate para facilitar
        -- buscas e APIs de frete. Formatações visuais (ex: '-' do CEP)
        -- podem ser feitas apenas na camada de visualização (BI) ou Trusted
        , cast(
            lpad(regexp_replace(postal_code, '[^0-9]', ''), 8, '0') as string
        ) as postal_code

        , cast(street as string) as street
        , cast(number as string) as location_number
        , cast(complement as string) as complement
        , cast(district as string) as district
        , cast(city as string) as city
        , cast(state as string) as location_state
        , cast(country as string) as country
        , cast(is_active as boolean) as is_active
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
