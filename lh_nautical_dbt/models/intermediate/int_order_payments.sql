-- models/intermediate/int_payments_aggregated_by_order.sql

with payments as (
    select * from {{ ref('stg_lh_nautical_payments') }}
),

aggregated as (
    select
        order_id
        
        -- 1. Agregamos os valores
        , sum(payment_amount) as total_paid_amount
        , count(payment_id) as total_installments
        
        -- 2. Descobrimos se envolveu cartão de crédito (para a Dor II)
        , max(case when lower(trim(payment_method)) = 'credit_card' then 1 else 0 end) as has_credit_card
        
        -- 3. (Opcional) Pegamos o método de pagamento principal (ex: se pagou parte em PIX e parte em Cartão)
        , max(payment_method) as main_payment_method 

    from payments
    group by order_id
)

select 
    order_id
    , total_paid_amount
    , total_installments
    , main_payment_method
    
    -- A Mágica para o seu Card no Power BI (KPI 4):
    -- Se tem cartão E foi mais de 1 parcela = Precisa Conciliar!
    , case 
        when has_credit_card = 1 and total_installments > 1 then true
        else false
      end as is_installment_to_reconcile

from aggregated