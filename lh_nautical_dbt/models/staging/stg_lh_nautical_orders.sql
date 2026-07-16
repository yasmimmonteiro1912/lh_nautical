with source_data as (

    select *
    from {{ source('lh_nautical', 'orders') }}

)

, renamed as (

    select
        cast(id as int) as order_id
        , cast(order_number as string) as order_number
        , cast(channel as string) as channel
        , cast(customer_id as int) as customer_id
        , cast(salesperson_id as int) as salesperson_id
        , cast(location_id as int) as location_id
        , cast(status as string) as status
        , cast(subtotal as decimal(14, 2)) as subtotal

        -- Garante que se o desconto for nulo, ele seja tratado como 0.00.
        , cast(
            coalesce(discount_amount, 0.00) as decimal(14, 2)
        ) as discount_amount

        , cast(total as decimal(14, 2)) as total
        , cast(placed_at as timestamp) as placed_at
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select *
from renamed
