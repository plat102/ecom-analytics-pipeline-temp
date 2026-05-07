{{ config(materialized='view', schema='intermediate') }}

/*
Intermediate: Outlier Summary

Purpose:
  - Provide high-level metrics on detected outliers
  - Monitor data quality over time
  - Help decide if outlier detection thresholds need adjustment
*/

WITH total_orders AS (
    SELECT COUNT(DISTINCT order_id) AS total_count
    FROM {{ ref('fact_sales_order_line') }}
),

outlier_breakdown AS (
    SELECT
        COUNT(DISTINCT order_id) AS total_outlier_orders,
        SUM(CASE WHEN is_placeholder_quantity THEN 1 ELSE 0 END) AS placeholder_quantity_count,
        SUM(CASE WHEN is_high_quantity_outlier THEN 1 ELSE 0 END) AS high_quantity_count,
        SUM(CASE WHEN is_high_value_outlier THEN 1 ELSE 0 END) AS high_value_count,
        SUM(CASE WHEN is_suspicious_pattern THEN 1 ELSE 0 END) AS suspicious_pattern_count,
        ROUND(SUM(total_order_value_usd), 2) AS total_outlier_revenue_usd,
        ROUND(AVG(total_order_value_usd), 2) AS avg_outlier_order_value_usd
    FROM {{ ref('int_dq_sales_outliers') }}
)

SELECT
    o.total_outlier_orders,
    o.placeholder_quantity_count,
    o.high_quantity_count,
    o.high_value_count,
    o.suspicious_pattern_count,
    o.total_outlier_revenue_usd,
    o.avg_outlier_order_value_usd,
    t.total_count AS total_orders_in_dataset,
    ROUND(100.0 * o.total_outlier_orders / t.total_count, 2) AS pct_of_total_orders
FROM outlier_breakdown o
CROSS JOIN total_orders t
