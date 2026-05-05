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
    e.current_url,
    e.is_recommendation_influenced,
    CAST(item.product_id AS STRING) AS product_id,
    item.amount AS quantity,
    item.price AS raw_price_string,
    COALESCE(item.currency, '') AS transaction_currency_symbol,
    CASE
      WHEN item.price LIKE '%.%,%' THEN
        SAFE_CAST(
          REPLACE(REPLACE(item.price, '.', ''), ',', '.') AS NUMERIC
        )
      WHEN item.price LIKE '%,%.%' THEN
        SAFE_CAST(REPLACE(item.price, ',', '') AS NUMERIC)
      WHEN item.price LIKE '%,%' THEN
        SAFE_CAST(REPLACE(item.price, ',', '.') AS NUMERIC)
      WHEN item.price LIKE '%.%' THEN
        SAFE_CAST(item.price AS NUMERIC)
      ELSE
        SAFE_CAST(item.price AS NUMERIC)
    END AS transaction_price,
    COALESCE(e.user_id_db, e.device_id) AS customer_natural_key,
    loc.country_name,
    loc.region_name,
    loc.city_name,
  FROM {{ ref('int_events_checkout_success') }} e,
  UNNEST(cart_products) AS item
  LEFT JOIN {{ ref('stg_ip_locations') }} loc
    ON e.ip = loc.ip
),

int_event__map_currency AS (
  SELECT
    c.*,
    COALESCE(er.currency_code, 'UNKNOWN') AS transaction_currency_code,
    COALESCE(er.rate_to_usd, 1.0) AS transaction_rate_to_usd
  FROM int_event__unnest_cart c
  LEFT JOIN {{ ref('exchange_rates') }} er
    ON c.transaction_currency_symbol = er.currency_symbol
    AND (
      er.url_pattern IS NULL
      OR er.url_pattern = ''
      OR c.current_url LIKE er.url_pattern
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.event_id, c.product_id
    ORDER BY COALESCE(er.priority, 999) ASC
  ) = 1
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
    transaction_price,
    transaction_currency_symbol,
    transaction_currency_code,
    transaction_rate_to_usd,
    is_recommendation_influenced,
    customer_natural_key,
  FROM int_event__map_currency
),

final AS (
  SELECT * FROM int_event__select_field
)


SELECT * FROM final
