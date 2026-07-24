with stg_movements as (
    select * from {{ ref('stg_lh_nautical_stock_movements') }}
),

sanitized_movements as (
    select
        -- 1. Identificadores e Colunas Puras (Regra ST06 do SQLFluff)
        mov.stock_movement_id
        , mov.product_variant_id
        , mov.location_id
        , mov.employee_id
        , mov.reference_id
        , mov.reference_table
        , mov.occurred_at
        , mov.ingestion_timestamp

        -- 2. Tratamento da coluna de movimentação
        , lower(trim(mov.movement_type)) as movement_type

        -- 3. Padronização e Tratamento de Nulos da Nota (movement_notes)
        , case
            when
                mov.movement_notes is null or trim(mov.movement_notes) = ''
                then 'sem_nota'
            when
                lower(trim(mov.movement_notes)) like '%revisa%'
                then 'necessita_revisao'
            when
                lower(trim(mov.movement_notes)) like '%saldo%inicia%'
                then 'saldo_inicial'
            when
                lower(trim(mov.movement_notes)) like '%conferido%'
                then 'conferido'
            else lower(trim(mov.movement_notes))
        end as movement_note_status

        -- 4. Flags de Auditoria e Qualidade
        , coalesce(
            lower(trim(mov.movement_notes)) like '%revisa%'
            , false
        ) as is_pending_review

        , coalesce(
            lower(trim(mov.movement_notes)) in (
                'conferido'
                , 'saldo_inicial'
            )
            , false
        ) as is_audited
        
        -- 5. Métrica Bruta com Sinal (Entrada + / Saída -)
        , case
            when
                lower(trim(mov.movement_type)) like '%out%'
                then -1 * abs(mov.quantity)
            else abs(mov.quantity)
        end as signed_quantity

        -- 6. Métrica Auditada
        , case
            when lower(trim(mov.movement_notes)) like '%revisa%' then 0
            when
                lower(trim(mov.movement_type)) like '%out%'
                then -1 * abs(mov.quantity)
            else abs(mov.quantity)
        end as audited_quantity

    from stg_movements as mov
)

select * from sanitized_movements
