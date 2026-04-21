{{
  config(
    materialized='view'
  )
}}

WITH items_unnested AS (
  SELECT
    event_id,
    event_timestamp,
    event_date,
    store_id,
    user_id_db,
    device_id,
    ip,
    device_category,
    browser,
    os,
    order_id,
    is_recommendation_influenced,
    CAST(item.product_id AS STRING) AS product_id,
    item.amount AS quantity,
    SAFE_CAST(item.price AS NUMERIC) AS unit_price,
    COALESCE(item.currency, currency_code) AS currency_code,
    COALESCE(user_id_db, device_id) AS customer_natural_key,
  FROM {{ ref('int_checkout_events_with_device') }},
  UNNEST(cart_products) AS item
),

items_selected AS (
  SELECT
    event_id,
    event_timestamp,
    event_date,
    store_id,
    user_id_db,
    device_id,
    ip,
    device_category,
    browser,
    os,
    order_id,
    product_id,
    quantity,
    unit_price,
    currency_code,
    is_recommendation_influenced,
    customer_natural_key,
  FROM items_unnested
),

final AS (
  SELECT * FROM items_selected
)


SELECT * FROM final
