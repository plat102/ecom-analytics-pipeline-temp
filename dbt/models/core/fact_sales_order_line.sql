{{
  config(
    materialized='table',
    partition_by={
      'field': 'event_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['store_id', 'product_key', 'customer_key']
  )
}}

WITH int_event__join_dimension AS (
  SELECT
    ci.event_id,
    ci.event_timestamp,
    ci.event_date,
    ci.order_id,
    ci.store_id,
    ci.product_id,
    ci.customer_natural_key,
    ci.ip,
    ci.country_name,
    ci.region_name,
    ci.city_name,
    ci.device_category,
    ci.browser,
    ci.os,
    ci.currency_code,
    ci.quantity,
    ci.unit_price,
    ci.is_recommendation_influenced,
    dd.date_key,
    dp.product_key,
    dc.customer_key,
    dl.location_key,
    ddev.device_key,
    dex.exchange_rate_key,
    dex.rate_to_usd,
  FROM {{ ref('int_events_cart_items') }} ci
  LEFT JOIN {{ ref('dim_date') }} dd
    ON ci.event_date = dd.full_date
  LEFT JOIN {{ ref('dim_product') }} dp
    ON ci.product_id = dp.product_id
  LEFT JOIN {{ ref('dim_customer') }} dc
    ON ci.customer_natural_key = dc.customer_natural_key
    AND dc.is_current = TRUE
  LEFT JOIN {{ ref('dim_location') }} dl
    ON ci.country_name = dl.country_name
    AND COALESCE(ci.region_name, '') = COALESCE(dl.region_name, '')
    AND COALESCE(ci.city_name, '') = COALESCE(dl.city_name, '')
  LEFT JOIN {{ ref('dim_device') }} ddev
    ON ci.device_category = ddev.device_category
    AND ci.browser = ddev.browser
    AND ci.os = ddev.os
  LEFT JOIN {{ ref('dim_exchange_rate') }} dex
    ON ci.currency_code = dex.currency_code
),

int_event__calculate_metric AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY event_timestamp, event_id, product_key) AS sales_line_key,
    event_date,
    -- Substitute NULL foreign keys with Unknown member keys (-1)
    COALESCE(date_key, 19000101) AS date_key,
    COALESCE(product_key, -1) AS product_key,
    COALESCE(customer_key, -1) AS customer_key,
    COALESCE(location_key, -1) AS location_key,
    COALESCE(device_key, -1) AS device_key,
    COALESCE(exchange_rate_key, -1) AS exchange_rate_key,
    event_id,
    order_id,
    store_id,
    ip,
    quantity,
    unit_price,
    currency_code,
    quantity * unit_price AS line_total,
    quantity * unit_price * COALESCE(rate_to_usd, 1.0) AS line_total_usd,
    is_recommendation_influenced,
    event_timestamp AS order_timestamp
  FROM int_event__join_dimension
),

final AS (
  SELECT * FROM int_event__calculate_metric
)


SELECT * FROM final
