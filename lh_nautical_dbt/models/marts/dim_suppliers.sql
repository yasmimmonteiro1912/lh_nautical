-- =====================================================================
-- Model: dim_suppliers
-- Description: Dimension table containing supplier master data.
-- =====================================================================

with stg_suppliers as (
    select * from {{ ref('stg_lh_nautical_suppliers') }}
),

transformed_suppliers as (
    select
        -- Primary Surrogate Key
        {{ dbt_utils.generate_surrogate_key([
            'supl.supplier_id'
        ]) }} as supplier_sk

        -- Natural Key
        , supl.supplier_id

        -- Dimensional Attributes
        , supl.legal_name
        , coalesce(
            supl.trade_name, supl.legal_name
        ) as trade_name
        , supl.tax_id
        , supl.tax_id_type
        , supl.contact_name
        , supl.email
        , supl.phone
        , supl.country
        , supl.supplier_is_active as is_active

        -- Metadata
        , supl.ingestion_timestamp
    from stg_suppliers as supl
)

select
    supplier_sk
    , supplier_id
    , legal_name
    , trade_name
    , tax_id
    , tax_id_type
    , contact_name
    , email
    , phone
    , country
    , is_active
    , ingestion_timestamp
from transformed_suppliers
