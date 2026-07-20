-- STREAMING_CHUNK:Generating raw dates using dbt_date...
with dates_raw as (
    -- Chamamos a macro que gera as datas básicas
    {{ dbt_date.get_date_dimension("2020-01-01", "2035-12-31") }}
)

-- STREAMING_CHUNK:Renaming and formatting columns with leading commas...
, dates_formatted as (
    select
        -- Surrogate Key inteligente (YYYYMMDD) ideal para SQL Server 
        -- e otimizada para Joins
        cast(date_format(date_day, 'yyyyMMdd') as int) as date_sk
        
        -- Datas básicas de referência
        , date_day as full_date
        , prior_date_day as previous_date
        , next_date_day as next_date
        , prior_year_date_day as prior_year_date
        , prior_year_over_year_date_day as prior_yoy_date
        
        -- Detalhes sobre o Dia (Alinhado com o protótipo image_9466af.png)
        , day_of_month as day -- noqa: RF04
        , day_of_year
        , day_of_week
        , day_of_week_iso
        , day_of_week_name as day_name
        , day_of_week_name_short as day_name_short
        
        -- Detalhes sobre a Semana
        , week_start_date
        , week_end_date
        , week_of_year
        , prior_year_week_start_date
        , prior_year_week_end_date
        
        -- Semana Padrão ISO
        , iso_week_start_date
        , iso_week_end_date
        , prior_year_iso_week_start_date
        , prior_year_iso_week_end_date
        
        -- Mês, Trimestre e Ano 
        , month_of_year as month -- noqa: RF04
        , month_name
        , month_name_short
        , quarter_of_year as quarter -- noqa: RF04
        , year_number as year -- noqa: RF04
        
        -- Flag booleana de Final de Semana 
        , case 
            when day_of_week in (1, 7) then 'Yes'
            else 'No'
        end as is_weekend
        
    from dates_raw
)

-- STREAMING_CHUNK:Returning all data from final CTE...
-- Retornando todos os dados limpos da última CTE
select * from dates_formatted
