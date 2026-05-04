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
Mart: Product Performance Metrics

Purpose:
  - Pre-aggregated product-level performance metrics by date
  - Serves Dashboard 4 (Product Performance)
  - Enables product analysis by category, collection, type

Grain: 1 row = 1 product × 1 date
*/

WITH
int_sale__select AS (

    SELECT
        order_date,
        order_id,
        product_key,
        quantity,
        unit_price,
        line_total_usd

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
        product_type,
        category_id,
        collection_name

    FROM {{ ref('dim_product') }}

),

int_sale__join_product AS (

    SELECT
        s.order_date,
        s.order_id,
        s.quantity,
        s.unit_price,
        s.line_total_usd,
        p.product_id,
        p.product_name,
        p.product_type,
        p.category_id,
        p.collection_name

    FROM int_sale__select s
    INNER JOIN dim_product__select p
        ON s.product_key = p.product_key

),

int_sale__aggregate AS (

    SELECT
        order_date,
        product_id,
        product_name,
        product_type,
        category_id,
        collection_name,

        -- Order metrics
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(*) AS line_item_count,

        -- Quantity metrics
        SUM(quantity) AS units_sold,

        -- Revenue metrics
        SUM(line_total_usd) AS revenue_usd,

        -- Price metrics
        AVG(unit_price) AS avg_unit_price

    FROM int_sale__join_product
    GROUP BY
        order_date,
        product_id,
        product_name,
        product_type,
        category_id,
        collection_name

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
        a.product_type,
        a.category_id,
        a.collection_name,

        -- Calendar attributes
        d.year,
        d.month,

        -- Metrics
        a.order_count,
        a.line_item_count,
        a.units_sold,
        a.revenue_usd,
        a.avg_unit_price

    FROM int_sale__aggregate a
    LEFT JOIN dim_date__get_calendar_field d
        ON a.order_date = d.full_date

),

final AS (

    SELECT * FROM int_sale__join_calendar

)

SELECT * FROM final
