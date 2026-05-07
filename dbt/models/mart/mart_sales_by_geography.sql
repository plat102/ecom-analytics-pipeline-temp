{{
    config(
        materialized='incremental',
        unique_key=['country_name', 'city_name', 'order_date'],
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['country_name'],
        incremental_strategy='insert_overwrite',
        schema='mart'
    )
}}

{#
  Grain: country_name × city_name × order_date
  Serves: Dashboard 2 (Geographic Analysis) - both country-level map and city-level table
  Looker Studio aggregates to country grain at query time when city is not selected.

  Future scaling note:
  If row count exceeds ~5M rows (approx. 50+ countries × dense city coverage × 3+ years),
  consider splitting into:
    - mart_sales_by_country (country × date) for map tiles
    - mart_sales_by_city (city × date) for detail tables
  Monitor with: SELECT COUNT(*) FROM mart.mart_sales_by_geography
#}

/*
Mart: Sales by Geography

Purpose:
  - Pre-aggregated sales metrics by country, city, and date
  - Serves Dashboard 2 (Geographic Analysis) - Geo map and city rankings
  - Looker Studio aggregates to country level at query time for map visualizations

Grain: 1 row = 1 country × 1 city × 1 date
*/

WITH
int_sale__select AS (

    SELECT
        order_date,
        order_id,
        customer_key,
        location_key,
        quantity,
        line_total_usd,
        is_outlier

    FROM {{ ref('int_sales_with_customer') }}

    WHERE is_outlier = FALSE

    {% if is_incremental() %}
    AND order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}

),

dim_location__select AS (

    SELECT
        location_key,
        country_name,
        region_name,
        city_name

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
        l.city_name

    FROM int_sale__select s
    LEFT JOIN dim_location__select l
        ON s.location_key = l.location_key

    -- FILTER: Only include rows with city data (avoid sparse data)
    WHERE l.city_name IS NOT NULL

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
