{{
    config(
        materialized='table',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['store_id', 'customer_natural_key']
    )
}}

/*
Intermediate model: Sales with Customer Data

Purpose:
  - Resolve dim_customer SCD Type 2 join ONCE (expensive operation)
  - Add order_date field for partitioning downstream marts
  - Serve as base for mart models

Grain: 1 row = 1 line item (same as fact_sales_order_line)
*/

WITH
fact_sale__select AS (

    SELECT
        sales_line_key,
        order_id,
        store_id,
        quantity,
        unit_price,
        line_total,
        line_total_usd,
        currency_code,
        ip,
        date_key,
        customer_key,
        product_key,
        location_key,
        device_key,
        exchange_rate_key

    FROM {{ ref('fact_sales_order_line') }}

),

dim_date__get_full_date AS (

    SELECT
        date_key,
        full_date AS order_date

    FROM {{ ref('dim_date') }}

),

dim_customer__get_current AS (

    SELECT
        customer_key,
        customer_natural_key,
        email_address,
        CASE
            WHEN email_address IS NULL THEN TRUE
            ELSE FALSE
        END AS is_guest

    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE

),

final AS (

    SELECT
        s.sales_line_key,
        s.order_id,
        s.store_id,
        s.quantity,
        s.unit_price,
        s.line_total,
        s.line_total_usd,
        s.currency_code,
        s.ip,
        s.date_key,
        d.order_date,
        s.customer_key,
        c.customer_natural_key,
        c.email_address,
        c.is_guest,
        s.product_key,
        s.location_key,
        s.device_key,
        s.exchange_rate_key

    FROM fact_sale__select s
    LEFT JOIN dim_date__get_full_date d
        ON s.date_key = d.date_key
    LEFT JOIN dim_customer__get_current c
        ON s.customer_key = c.customer_key

)

SELECT * FROM final
