{{
    config(
        materialized='incremental',
        unique_key=['store_id', 'order_date'],
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['store_id'],
        incremental_strategy='insert_overwrite',
        schema='mart'
    )
}}

/*
Mart: Daily Revenue by Store

Purpose:
  - Pre-aggregated daily revenue metrics by store
  - Serves Dashboard 1 (Revenue Overview) and Dashboard 3 (Temporal Analysis)
  - Dashboard 3 uses this mart but aggregates by removing store dimension

Grain: 1 row = 1 store × 1 date
*/

WITH
int_sale__select AS (

    SELECT
        order_date,
        store_id,
        order_id,
        customer_key,
        quantity,
        line_total_usd,
        is_guest

    FROM {{ ref('int_sales_with_customer') }}

    {% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}

),

dim_date__get_calendar_field AS (

    SELECT
        full_date,
        year,
        quarter,
        month,
        week AS week_of_year,
        day_of_week,
        is_weekend

    FROM {{ ref('dim_date') }}

),

int_sale__aggregate AS (

    SELECT
        s.order_date,
        s.store_id,

        -- Order metrics
        COUNT(DISTINCT s.order_id) AS order_count,
        COUNT(*) AS line_item_count,

        -- Quantity metrics
        SUM(s.quantity) AS units_sold,

        -- Revenue metrics
        SUM(s.line_total_usd) AS revenue_usd,

        -- Customer metrics
        COUNT(DISTINCT s.customer_key) AS unique_customers,

        -- Customer type breakdown
        COUNTIF(s.is_guest = TRUE) AS guest_orders,
        COUNTIF(s.is_guest = FALSE) AS registered_orders

    FROM int_sale__select s
    GROUP BY
        s.order_date,
        s.store_id

),

int_sale__join_calendar AS (

    SELECT
        a.order_date,
        a.store_id,

        -- Calendar attributes
        d.year,
        d.quarter,
        d.month,
        d.week_of_year,
        d.day_of_week,
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
