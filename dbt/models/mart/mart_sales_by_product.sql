{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'order_date'],
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['category_id', 'collection_name'],
        incremental_strategy='insert_overwrite',
        schema='mart'
    )
}}

/*
Mart: Product Sales Performance

Purpose:
  - Pre-aggregated product-level sales metrics by date
  - Serves Dashboard 4 (Product Performance)
  - Enables product analysis by category, collection, type, and customer segment

Grain: 1 row = 1 product × 1 date (only days with sales)
*/

WITH
int_sale__select AS (

    SELECT
        order_date,
        order_id,
        product_key,
        customer_key,
        quantity,
        unit_price,
        line_total_usd,
        is_guest,
        email_address

    FROM {{ ref('int_sales_with_customer') }}

    {% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}

),

dim_product__select AS (

    SELECT
        product_key,
        product_id,
        product_name,
        sku,
        product_type,
        category_id,
        collection_name,
        gender,
        price AS catalog_price,
        min_price AS catalog_min_price,
        max_price AS catalog_max_price,
        gold_weight,
        currency_code AS catalog_currency

    FROM {{ ref('dim_product') }}

),

int_sale__join_product AS (

    SELECT
        s.order_date,
        s.order_id,
        s.customer_key,
        s.quantity,
        s.unit_price,
        s.line_total_usd,
        s.is_guest,
        s.email_address,
        p.product_id,
        p.product_name,
        p.sku,
        p.product_type,
        p.category_id,
        p.collection_name,
        p.gender,
        p.catalog_price,
        p.catalog_min_price,
        p.catalog_max_price,
        p.gold_weight,
        p.catalog_currency

    FROM int_sale__select s
    INNER JOIN dim_product__select p
        ON s.product_key = p.product_key

),

int_sale__aggregate AS (

    SELECT
        order_date,
        product_id,
        product_name,
        sku,
        product_type,
        category_id,
        -- Derive category_name from category_id (placeholder - can be enhanced with actual mapping)
        CAST(category_id AS STRING) AS category_name,
        collection_name,
        gender,
        catalog_price,
        catalog_min_price,
        catalog_max_price,
        gold_weight,
        catalog_currency,

        -- Order metrics
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(*) AS line_item_count,

        -- Quantity metrics
        SUM(quantity) AS units_sold,

        -- Revenue metrics
        SUM(line_total_usd) AS revenue_usd,

        -- Price metrics
        AVG(unit_price) AS avg_unit_price,
        MIN(unit_price) AS min_unit_price,
        MAX(unit_price) AS max_unit_price,

        -- Customer segment metrics (NEW)
        COUNTIF(is_guest = FALSE) AS registered_customer_orders,
        COUNTIF(is_guest = TRUE) AS guest_orders,
        COUNT(DISTINCT CASE
            WHEN email_address IS NOT NULL
            THEN REGEXP_EXTRACT(email_address, r'@(.+)$')
            ELSE NULL
        END) AS unique_email_domains

    FROM int_sale__join_product
    GROUP BY
        order_date,
        product_id,
        product_name,
        sku,
        product_type,
        category_id,
        collection_name,
        gender,
        catalog_price,
        catalog_min_price,
        catalog_max_price,
        gold_weight,
        catalog_currency

    -- FILTER: Only include days with actual sales (avoids sparse data)
    HAVING order_count > 0

),

dim_date__get_calendar_field AS (

    SELECT
        full_date,
        year,
        month

    FROM {{ ref('dim_date') }}

),

int_sale__join_calendar AS (

    SELECT
        a.order_date,
        a.product_id,
        a.product_name,
        a.sku,
        a.product_type,
        a.category_id,
        a.category_name,
        a.collection_name,
        a.gender,

        -- Product attributes (catalog)
        a.catalog_price,
        a.catalog_min_price,
        a.catalog_max_price,
        a.gold_weight,
        a.catalog_currency,

        -- Calendar attributes
        d.year,
        d.month,

        -- Metrics
        a.order_count,
        a.line_item_count,
        a.units_sold,
        a.revenue_usd,
        a.avg_unit_price,
        a.min_unit_price,
        a.max_unit_price,

        -- Customer segment metrics
        a.registered_customer_orders,
        a.guest_orders,
        a.unique_email_domains

    FROM int_sale__aggregate a
    LEFT JOIN dim_date__get_calendar_field d
        ON a.order_date = d.full_date

),

final AS (

    SELECT * FROM int_sale__join_calendar

)

SELECT * FROM final
