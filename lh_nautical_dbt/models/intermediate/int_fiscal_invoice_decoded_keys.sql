-- =====================================================================
-- Model: int_fiscal_invoice_decoded_keys
-- Description: Decodes the 44-digit Brazilian NF-e Access Key stored at the
--              end of the S3 URI path to extract metadata like state, CNPJ, and series.
-- =====================================================================

with staging_fiscal_invoices as (

    select *
    from {{ ref('stg_lh_nautical_fiscal_invoices') }}

)

-- Extracting the raw 44-digit key from the end of the S3 path
, extracted_key as (

    select
          fiscal_invoice_id
        , xml_storage_uri
        -- Grabs the last 44 characters of the URI string (assuming no file extension)
        , right(trim(xml_storage_uri), 44) as nfe_access_key
    -- Alias 'sfi' meets the minimum 3-character requirement
    from staging_fiscal_invoices as sfi

)

-- Parsing the structural positions of the NF-e access key
, decoded_key as (

    select
          fiscal_invoice_id
        , xml_storage_uri
        , nfe_access_key
        -- State/Region code (e.g., '31' for MG, '35' for SP, '32' for ES)
        , substring(nfe_access_key, 1, 2) as emitter_state_code
        -- Emission Date in YYMM format
        , substring(nfe_access_key, 3, 4) as emission_year_month
        -- Taxpayer identification (CNPJ) of the issuing store/warehouse
        , substring(nfe_access_key, 7, 14) as emitter_cnpj
        -- Fiscal document model (e.g., '55' for standard NF-e)
        , substring(nfe_access_key, 21, 2) as fiscal_document_model
        -- Document series code
        , substring(nfe_access_key, 23, 3) as fiscal_document_series
        -- Sequential invoice number
        , substring(nfe_access_key, 26, 9) as fiscal_document_number
        -- Emission type indicator (e.g., '1' for normal, '9' for contingency)
        , substring(nfe_access_key, 35, 1) as emission_type_code
    -- Alias 'ext' meets the minimum 3-character requirement
    from extracted_key as ext

)

-- Final CTE to structure the selected output in sequential order
, final as (

    select
          fiscal_invoice_id
        , xml_storage_uri
        , nfe_access_key
        , emitter_state_code
        , emission_year_month
        , emitter_cnpj
        , fiscal_document_model
        , fiscal_document_series
        , fiscal_document_number
        , emission_type_code
    -- Alias 'dec' meets the minimum 3-character requirement
    from decoded_key as dec

)

select
      fiscal_invoice_id
    , xml_storage_uri
    , nfe_access_key
    , emitter_state_code
    , emission_year_month
    , emitter_cnpj
    , fiscal_document_model
    , fiscal_document_series
    , fiscal_document_number
    , emission_type_code

from final as fin