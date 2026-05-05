{{
    config(
        materialized='table',
        cluster_by=['email_domain', 'is_guest', 'purchase_frequency_segment'],
        schema='mart'
    )
}}

/*
Mart: Customer Lifetime Summary

Purpose:
  - Customer lifetime aggregates for RFM segmentation, cohort analysis, and LTV
  - Serves Dashboard 5 (Customer Behavior Analysis)
  - Enables customer segmentation and retention analysis

Grain: 1 row = 1 customer (aggregated lifetime metrics)
*/

WITH
int_sale__select AS (

    SELECT
        order_date,
        order_id,
        customer_key,
        device_key,
        location_key,
        quantity,
        line_total_usd,
        customer_natural_key,
        email_address,
        is_guest

    FROM {{ ref('int_sales_with_customer') }}

),

dim_device__select AS (

    SELECT
        device_key,
        device_category

    FROM {{ ref('dim_device') }}

),

dim_location__select AS (

    SELECT
        location_key,
        country_name

    FROM {{ ref('dim_location') }}

),

int_sale__join_dimensions AS (

    SELECT
        s.order_date,
        s.order_id,
        s.customer_key,
        s.quantity,
        s.line_total_usd,
        s.customer_natural_key,
        s.email_address,
        s.is_guest,
        d.device_category,
        l.country_name

    FROM int_sale__select s
    LEFT JOIN dim_device__select d
        ON s.device_key = d.device_key
    LEFT JOIN dim_location__select l
        ON s.location_key = l.location_key

),

customer__derive_email_domain AS (

    SELECT
        customer_key,
        customer_natural_key,
        email_address,
        is_guest,
        CASE
            WHEN email_address IS NULL THEN 'Guest'
            ELSE REGEXP_EXTRACT(email_address, r'@(.+)$')
        END AS email_domain

    FROM int_sale__join_dimensions
    GROUP BY
        customer_key,
        customer_natural_key,
        email_address,
        is_guest

),

customer__aggregate_metrics AS (

    SELECT
        s.customer_key,

        -- Temporal metrics
        MIN(s.order_date) AS first_order_date,
        MAX(s.order_date) AS last_order_date,
        DATE_DIFF(MAX(s.order_date), MIN(s.order_date), DAY) AS days_between_first_last,
        DATE_DIFF(CURRENT_DATE(), MAX(s.order_date), DAY) AS days_since_last_order,

        -- Purchase behavior (RFM metrics)
        COUNT(DISTINCT s.order_id) AS total_orders,
        SUM(s.line_total_usd) AS total_revenue_usd,
        SUM(s.quantity) AS total_units_purchased,

        -- Device preference (most common device)
        APPROX_TOP_COUNT(s.device_category, 1)[OFFSET(0)].value AS primary_device_category,

        -- Geographic preference (most common country)
        APPROX_TOP_COUNT(s.country_name, 1)[OFFSET(0)].value AS primary_country

    FROM int_sale__join_dimensions s
    GROUP BY
        s.customer_key

),

customer__join_attributes AS (

    SELECT
        c.customer_key,
        c.customer_natural_key,
        c.email_address,
        c.email_domain,
        c.is_guest,

        -- Temporal metrics
        m.first_order_date,
        m.last_order_date,
        m.days_between_first_last,
        m.days_since_last_order,

        -- Purchase behavior
        m.total_orders,
        m.total_revenue_usd,
        SAFE_DIVIDE(m.total_revenue_usd, m.total_orders) AS avg_order_value,
        m.total_units_purchased,

        -- Preferences
        m.primary_device_category,
        m.primary_country,

        -- Segmentation
        CASE
            WHEN m.total_orders = 1 THEN 'One-time'
            WHEN m.total_orders BETWEEN 2 AND 5 THEN 'Repeat'
            ELSE 'Loyal'
        END AS purchase_frequency_segment

    FROM customer__derive_email_domain c
    INNER JOIN customer__aggregate_metrics m
        ON c.customer_key = m.customer_key

),

final AS (

    SELECT * FROM customer__join_attributes

)

SELECT * FROM final
