{{
  config(
    materialized='view'
  )
}}

WITH items_unnested AS (
  SELECT
    e.event_id,
    e.event_timestamp,
    e.event_date,
    e.store_id,
    e.user_id_db,
    e.device_id,
    e.ip,
    e.device_category,
    e.browser,
    e.os,
    e.order_id,
    e.is_recommendation_influenced,
    CAST(item.product_id AS STRING) AS product_id,
    item.amount AS quantity,
    SAFE_CAST(item.price AS NUMERIC) AS unit_price,
    COALESCE(item.currency, e.currency_code) AS currency_code,
    COALESCE(e.user_id_db, e.device_id) AS customer_natural_key,
    loc.country_name,
    loc.region_name,
    loc.city_name,
  FROM {{ ref('int_events_checkout_success') }} e,
  UNNEST(cart_products) AS item
  LEFT JOIN {{ ref('stg_ip_locations') }} loc
    ON e.ip = loc.ip
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
    country_name,
    region_name,
    city_name,
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
