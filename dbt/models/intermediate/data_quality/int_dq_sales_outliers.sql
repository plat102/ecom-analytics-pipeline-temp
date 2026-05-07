{{ config(
    materialized='table',
    schema='intermediate'
) }}

/*
Intermediate: Sales Outlier Detection

Purpose:
  - Identify orders with unusual patterns (high quantity, high value, test data)
  - Flag outliers instead of filtering them out
  - Allow downstream models to decide how to handle outliers

Grain: 1 row = 1 order with outlier flags

Detection Methods:
  1. Placeholder values: Common test quantities (9999, 999, 9998, 1000)
  2. Statistical outliers: P99 threshold for quantity and value
  3. Suspicious patterns: Multi-line orders with zero prices

Business Context:
  - Jewelry orders typically have quantity 1-10
  - High-value orders exist but quantities >100 are rare
  - Test orders from @glamira-group.com should be flagged
*/

WITH order_aggregates AS (
    SELECT
        order_id,
        SUM(quantity) AS total_quantity,
        SUM(line_total_usd) AS total_order_value_usd,
        COUNT(*) AS line_item_count,
        MAX(quantity) AS max_line_quantity,
        MAX(line_total_usd) AS max_line_value_usd,
        AVG(unit_price) AS avg_unit_price
    FROM {{ ref('fact_sales_order_line') }}
    GROUP BY order_id
),

quantity_percentiles AS (
    SELECT
        APPROX_QUANTILES(quantity, 100)[OFFSET(99)] AS p99_quantity,
        APPROX_QUANTILES(quantity, 100)[OFFSET(95)] AS p95_quantity
    FROM {{ ref('fact_sales_order_line') }}
),

value_percentiles AS (
    SELECT
        APPROX_QUANTILES(line_total_usd, 100)[OFFSET(99)] AS p99_value,
        APPROX_QUANTILES(line_total_usd, 100)[OFFSET(95)] AS p95_value
    FROM {{ ref('fact_sales_order_line') }}
),

outlier_detection AS (
    SELECT
        o.order_id,
        o.total_quantity,
        o.total_order_value_usd,
        o.max_line_quantity,
        o.max_line_value_usd,

        -- Flag: Placeholder/Test Quantity (exact match common test values)
        CASE
            WHEN o.max_line_quantity IN (9999, 999, 9998, 1000)
            THEN TRUE
            ELSE FALSE
        END AS is_placeholder_quantity,

        -- Flag: High Quantity Outlier (P99)
        CASE
            WHEN o.max_line_quantity > (SELECT p99_quantity FROM quantity_percentiles)
            THEN TRUE
            ELSE FALSE
        END AS is_high_quantity_outlier,

        -- Flag: High Value Outlier (P99)
        CASE
            WHEN o.total_order_value_usd > (SELECT p99_value FROM value_percentiles)
            THEN TRUE
            ELSE FALSE
        END AS is_high_value_outlier,

        -- Flag: Suspicious Pattern (free items in multi-line order)
        CASE
            WHEN o.line_item_count > 1
                AND o.avg_unit_price = 0
            THEN TRUE
            ELSE FALSE
        END AS is_suspicious_pattern,

        -- Combined Flag: Any outlier detected
        CASE
            WHEN o.max_line_quantity IN (9999, 999, 9998, 1000)
                OR o.max_line_quantity > (SELECT p99_quantity FROM quantity_percentiles)
                OR o.total_order_value_usd > (SELECT p99_value FROM value_percentiles)
            THEN TRUE
            ELSE FALSE
        END AS is_outlier

    FROM order_aggregates o
)

SELECT * FROM outlier_detection
WHERE is_outlier = TRUE
