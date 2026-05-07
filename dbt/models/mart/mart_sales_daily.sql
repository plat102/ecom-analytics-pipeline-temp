{{
    config(
        materialized='incremental',
        unique_key=['store_id', 'order_date', 'device_category'],
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['store_id', 'device_category'],
        incremental_strategy='insert_overwrite',
        schema='mart'
    )
}}

/*
Mart: Daily Sales by Store and Device

Purpose:
  - Pre-aggregated daily sales metrics by store and device category
  - Serves Dashboard 1 (Revenue Overview) and Dashboard 3 (Temporal Analysis)
  - Dashboard 3 uses this mart but aggregates by removing store dimension

Grain: 1 row = 1 store × 1 date × 1 device_category
*/

WITH
int_sale__select AS (

    SELECT
        order_date,
        store_id,
        order_id,
        customer_key,
        device_key,
        quantity,
        line_total_usd,
        is_guest,
        is_outlier

    FROM {{ ref('int_sales_with_customer') }}

    WHERE is_outlier = FALSE

    {% if is_incremental() %}
    AND order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}

),

dim_device__select AS (

    SELECT
        device_key,
        device_category,
        is_mobile

    FROM {{ ref('dim_device') }}

),

dim_date__get_calendar_field AS (

    SELECT
        full_date,
        year,
        quarter,
        month,
        week AS week_of_year,
        day_of_week,
        day_name,
        is_weekend

    FROM {{ ref('dim_date') }}

),

int_sale__join_device AS (

    SELECT
        s.order_date,
        s.store_id,
        s.order_id,
        s.customer_key,
        s.quantity,
        s.line_total_usd,
        s.is_guest,
        d.device_category,
        d.is_mobile

    FROM int_sale__select s
    LEFT JOIN dim_device__select d
        ON s.device_key = d.device_key

),

int_sale__aggregate AS (

    SELECT
        order_date,
        store_id,
        device_category,
        is_mobile,

        -- Order metrics
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(*) AS line_item_count,

        -- Quantity metrics
        SUM(quantity) AS units_sold,

        -- Revenue metrics
        SUM(line_total_usd) AS revenue_usd,

        -- Customer metrics
        COUNT(DISTINCT customer_key) AS unique_customers,

        -- Customer type breakdown
        COUNTIF(is_guest = TRUE) AS guest_orders,
        COUNTIF(is_guest = FALSE) AS registered_orders

    FROM int_sale__join_device
    GROUP BY
        order_date,
        store_id,
        device_category,
        is_mobile

),

int_sale__join_calendar AS (

    SELECT
        a.order_date,
        a.store_id,

        -- Device attributes
        a.device_category,
        a.is_mobile,

        -- Calendar attributes
        d.year,
        d.quarter,
        d.month,
        d.week_of_year,
        d.day_of_week,
        d.day_name,
        d.is_weekend,

        -- Metrics
        a.order_count,
        a.line_item_count,
        a.units_sold,
        a.revenue_usd,
        a.unique_customers,
        a.guest_orders,
        a.registered_orders

    FROM int_sale__aggregate a
    LEFT JOIN dim_date__get_calendar_field d
        ON a.order_date = d.full_date

),

final AS (

    SELECT * FROM int_sale__join_calendar

)

SELECT * FROM final
