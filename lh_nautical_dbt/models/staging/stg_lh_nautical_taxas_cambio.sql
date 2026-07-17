-- =====================================================================
-- Model: stg_lh_nautical_taxas_cambio
-- Description: Standardizes, cleans, and casts daily exchange rates.
--              - Renames mixedCase columns to snake_case.
--              - Converts rates to double and timestamps to proper format.
--              - Deduplicates records to ensure idempotency.
-- =====================================================================

with source_data as (

    select *
    from {{ source('lh_nautical', 'taxas_cambio') }}

)

, cleaned_records as (

    select
          cast(datahoracotacao as timestamp) as data_hora_cotacao
        , cast(cotacaocompra as double) as cotacao_compra_dolar
        , cast(cotacaovenda as double) as cotacao_venda_dolar
        , cast(cotacaocompraeuro as double) as cotacao_compra_euro
        , cast(cotacaovendaeuro as double) as cotacao_venda_euro
        , cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source_data

)

select
      data_hora_cotacao
    , cotacao_compra_dolar
    , cotacao_venda_dolar
    , cotacao_compra_euro
    , cotacao_venda_euro
    , ingestion_timestamp
from cleaned_records
-- 🛡️ Deduplicação definitiva: se houver datas idênticas, mantém apenas a de maior ingestion_timestamp
qualify row_number() over (
    partition by data_hora_cotacao
    order by ingestion_timestamp desc
) = 1