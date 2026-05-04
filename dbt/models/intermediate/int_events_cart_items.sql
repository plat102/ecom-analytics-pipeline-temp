{{
  config(
    materialized='view'
  )
}}

WITH int_event__unnest_cart AS (
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

int_event__select_field AS (
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
  FROM int_event__unnest_cart
),

final AS (
  SELECT * FROM int_event__select_field
)


SELECT * FROM final
