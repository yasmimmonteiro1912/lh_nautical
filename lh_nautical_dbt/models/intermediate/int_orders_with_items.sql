with orders as (

    select *
    from {{ ref('stg_lh_nautical_orders') }}

)

, order_items as (

    select *
    from {{ ref('stg_lh_nautical_order_items') }}

)

, aggregated_items as (

    select
        order_id
        , count(order_item_id) as total_items_count
        , sum(quantity) as total_quantity
        , sum(line_total) as calculated_subtotal
    from order_items
    group by order_id

)

, enriched_orders as (

    select
        -- Identificadores MANTIDOS COMO STRING (sem cast para int!)
        orders.order_id
        , orders.customer_id
        , orders.location_id
        , orders.order_number

        -- Atributos Padronizados
        , lower(trim(orders.status)) as order_status
        , lower(trim(orders.channel)) as order_channel

        -- Flags Booleanas
        , case 
            when lower(trim(orders.status)) in ('cancelled', 'canceled', 'void') then true 
            else false 
          end as is_cancelled

        , case 
            when lower(trim(orders.status)) in ('completed', 'paid', 'closed', 'finished', 'confirmed') then true 
            else false 
          end as is_completed

        -- Valores e Métricas
        , cast(orders.total as decimal(14, 2)) as order_total_amount
        , cast(orders.discount_amount as decimal(14, 2)) as discount_amount
        , coalesce(aggregated_items.total_items_count, 0) as total_items_count
        , coalesce(aggregated_items.total_quantity, 0) as total_quantity

        -- Datas
        , orders.placed_at
        , orders.created_at
        , orders.updated_at

    from orders
    left join aggregated_items
        on orders.order_id = aggregated_items.order_id

)

select *
from enriched_orders
