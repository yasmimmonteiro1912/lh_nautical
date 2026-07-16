with source_data as (

    select *
    from {{ source('lh_nautical', 'goods_receipts') }}

)

, renamed as (

    select
        cast(id as int) as goods_receipt_id
        , cast(purchase_order_id as int) as purchase_order_id
        , cast(received_by_employee_id as int) as received_by_employee_id
        , cast(received_at as timestamp) as received_at

        -- Limpeza das Notas: Remove espaços inúteis nas pontas e
        -- garante que campos vazios ou sem conteúdo virem NULL, 
        -- mesmo se usuário só apertou a barra de espaço. 
        -- Isso ajuda em análises de texto 
        -- (como "quantos recebimentos foram feitos SEM nenhuma observação?
        , cast(
            nullif(trim(notes), '') as string
        ) as notes

        , cast(created_at as timestamp) as created_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
