{% macro drop_staging_views() %}
    {% set statements = [
        "drop view if exists silver.stg_lh_nautical_purchase_order_items",
        "drop view if exists silver.stg_lh_nautical_purchase_orders",
        "drop view if exists silver.stg_lh_nautical_return_items",
        "drop view if exists silver.stg_lh_nautical_returns",
        "drop view if exists silver.stg_lh_nautical_stock_levels",
        "drop view if exists silver.stg_lh_nautical_stock_movements",
        "drop view if exists silver.stg_lh_nautical_suppliers",
        "drop view if exists silver.stg_lh_nautical_variant_attribute_values",
        "drop view if exists silver.stg_lh_nautical_addresses"
    ] %}

    {% for stmt in statements %}
        {% do log("Executando no Databricks: " ~ stmt, info=True) %}
        {% do run_query(stmt) %}
    {% endfor %}
{% endmacro %}