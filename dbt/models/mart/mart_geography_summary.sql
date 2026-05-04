{{
    config(
        materialized='incremental',
        unique_key=['country_name', 'region_name', 'city_name', 'order_date'],
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['country_name', 'region_name'],
        incremental_strategy='insert_overwrite',
        schema='mart'
    )
}}

/*
Mart: Geographic Revenue Summary

Purpose:
  - Pre-aggregated revenue metrics by geography (country, region, city) and date
  - Serves Dashboard 2 (Geographic Analysis)
  - Enables geographic drill-down analysis

Grain: 1 row = 1 country × 1 region × 1 city × 1 date
*/

WITH
int_sale__select AS (

    SELECT
        order_date,
        order_id,
        customer_key,
        location_key,
        quantity,
        line_total_usd

    FROM {{ ref('int_sales_with_customer') }}

    {% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}

),

dim_location__select AS (

    SELECT
        location_key,
        country_name,
        region_name,
        city_name,
        geo_completeness_level

    FROM {{ ref('dim_location') }}

),

int_sale__join_location AS (

    SELECT
        s.order_date,
        s.order_id,
        s.customer_key,
        s.quantity,
        s.line_total_usd,
        l.country_name,
        l.region_name,
        l.city_name,
        l.geo_completeness_level

    FROM int_sale__select s
    LEFT JOIN dim_location__select l
        ON s.location_key = l.location_key

),

int_sale__aggregate AS (

    SELECT
        order_date,
        country_name,
        region_name,
        city_name,

        -- Order metrics
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(*) AS line_item_count,

        -- Quantity metrics
        SUM(quantity) AS units_sold,

        -- Revenue metrics
        SUM(line_total_usd) AS revenue_usd,

        -- Customer metrics
        COUNT(DISTINCT customer_key) AS unique_customers

    FROM int_sale__join_location
    GROUP BY
        order_date,
        country_name,
        region_name,
        city_name

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
        a.country_name,
        a.region_name,
        a.city_name,

        -- Calendar attributes
        d.year,
        d.month,

        -- Metrics
        a.order_count,
        a.line_item_count,
        a.units_sold,
        a.revenue_usd,
        a.unique_customers

    FROM int_sale__aggregate a
    LEFT JOIN dim_date__get_calendar_field d
        ON a.order_date = d.full_date

),

final AS (

    SELECT * FROM int_sale__join_calendar

)

SELECT * FROM final
