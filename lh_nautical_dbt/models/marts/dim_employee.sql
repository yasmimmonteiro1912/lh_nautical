-- =====================================================================
-- Model: dim_employee
-- Description: Core employee dimension model matching official mart schema.
-- =====================================================================

with employees as (
    select * from {{ ref('stg_lh_nautical_employees') }}
),

locations as (
    select * from {{ ref('int_cep_locations') }}
),

joined_data as (
    select
        emp.employee_id
        , emp.employee_full_name
        , emp.employee_cpf
        , emp.employee_email
        , emp.employee_role
        , emp.hire_date
        , emp.termination_date
        , loc.location_name as primary_location_name
        , loc.location_type as primary_location_type
        , loc.city as primary_location_city
        , loc.location_state as primary_location_state
        , loc.postal_code as primary_location_postal_code
        , emp.ingestion_timestamp as emp_ingestion_ts
        , loc.ingestion_timestamp as loc_ingestion_ts
    from employees as emp
    left join locations as loc
        on emp.primary_location_id = loc.location_id
),

calculated_fields as (
    select
        -- 1. Colunas simples/diretas primeiro (Regra ST06 do SQLFluff)
        employee_id
        , employee_full_name
        , employee_cpf
        , employee_email
        , employee_role
        , hire_date
        , termination_date
        , primary_location_name
        , primary_location_type
        , primary_location_city
        , primary_location_state
        , primary_location_postal_code

        -- 2. Regra de Negocio: Ativo se nao tiver data de demissao
        , coalesce(termination_date is null, false) as is_active

        -- 3. Surrogate Key
        , {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_sk

        -- 4. Timestamp de auditoria consolidado
        , case
            when loc_ingestion_ts > emp_ingestion_ts then loc_ingestion_ts
            else emp_ingestion_ts
        end as ingestion_timestamp
    from joined_data
)

-- 5. SELECT final padronizado com SK no topo
select
    employee_sk
    , employee_id
    , employee_full_name
    , employee_cpf
    , employee_email
    , employee_role
    , hire_date
    , termination_date
    , is_active
    , primary_location_name
    , primary_location_type
    , primary_location_city
    , primary_location_state
    , primary_location_postal_code
    , ingestion_timestamp
from calculated_fields
