-- =====================================================================
-- Singular Test: assert_stock_value_coherence
-- Description: Valida se registros com quantidade de estoque nula ou negativa
--              possuem valor financeiro zerado (coerencia de negocio).
-- =====================================================================

select
    snapshot_sk
    , quantity_on_hand
    , stock_value_estimated
from {{ ref('fct_inventory_snapshot_daily') }}
where
    quantity_on_hand <= 0
    and stock_value_estimated > 0
