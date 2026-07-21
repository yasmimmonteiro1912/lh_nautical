-- =====================================================================
-- Model: dim_employee
-- Description: Enriched Employee Dimension for Role-Playing.
--              Consumes employees from Staging and location context
--              from the intermediate model 'int_cep_locations'.
-- =====================================================================

with employees as (
    select * from {{ ref('stg_lh_nautical_employees') }}
),

locations as (
    -- 🚨 ATUALIZADO: Consome a camada Intermediate da sua colega!
    select * from {{ ref('int_cep_locations') }}
),

joined_data as (
    select
        -- 1. Surrogate Key (Hash MD5)
        emp.employee_id
        
        -- 2. Atributos do Colaborador
        , emp.employee_full_name as full_name
        , emp.employee_cpf as cpf
        , emp.employee_email as email
        , emp.employee_role as role
        , emp.hire_date
        , emp.termination_date
        , loc.location_name as primary_location_name
        
        -- Regra de Negócio: Status ativo baseado na data de demissão
        , loc.location_type as primary_location_type
        
        -- 3. Atributos Desnormalizados da Filial Principal (vindos da int_cep_locations)
        , loc.city as primary_location_city
        , loc.location_state as primary_location_state
        , loc.postal_code as primary_location_postal_code
        , md5(cast(emp.employee_id as string)) as employee_key
        , coalesce(emp.termination_date is null, false) as employee_is_active
        
        -- 4. Timestamp de Auditoria
        , case
            when loc.ingestion_timestamp > emp.ingestion_timestamp
                then loc.ingestion_timestamp
            else emp.ingestion_timestamp
        end as ingestion_timestamp

    from employees as emp
    left join locations as loc
        on emp.primary_location_id = loc.location_id
)

-- SELECT final padronizado para o BI
select
    employee_key
    , employee_id
    , full_name
    , cpf
    , email
    , role
    , hire_date
    , termination_date
    , employee_is_active
    , primary_location_name
    , primary_location_type
    , primary_location_city
    , primary_location_state
    , primary_location_postal_code
    , ingestion_timestamp
from joined_data
