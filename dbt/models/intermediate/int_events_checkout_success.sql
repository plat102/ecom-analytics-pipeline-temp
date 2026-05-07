{{
  config(
    materialized='view'
  )
}}

WITH stg_event__filter_checkout AS (
  SELECT
    event_id,
    event_timestamp,
    event_date,
    event_type,
    store_id,
    user_id_db,
    device_id,
    ip_address,
    user_agent,
    current_url,
    order_id,
    cart_products,
    currency_code,
    is_recommendation_influenced,
    ingested_at,
  FROM {{ ref('stg_events') }}
  WHERE event_type = 'checkout_success'
    AND order_id IS NOT NULL
    AND cart_products IS NOT NULL
    AND user_agent IS NOT NULL
),

stg_event__parse_device AS (
  SELECT
    event_id,
    event_timestamp,
    event_date,
    event_type,
    store_id,
    user_id_db,
    device_id,
    ip_address,
    user_agent,
    current_url,
    order_id,
    cart_products,
    currency_code,
    is_recommendation_influenced,
    ingested_at,
    CASE
      WHEN user_agent LIKE '%iPhone%' OR user_agent LIKE '%Android%' THEN 'Mobile'
      WHEN user_agent LIKE '%iPad%' THEN 'Tablet'
      ELSE 'Desktop'
    END AS device_category,
    CASE
      WHEN user_agent LIKE '%Chrome%' AND user_agent NOT LIKE '%Edg/%' THEN 'Chrome'
      WHEN user_agent LIKE '%Safari%' AND user_agent NOT LIKE '%Chrome%' THEN 'Safari'
      WHEN user_agent LIKE '%Firefox%' THEN 'Firefox'
      WHEN user_agent LIKE '%Edg/%' THEN 'Edge'
      ELSE 'Other'
    END AS browser_name,
    CASE
      WHEN user_agent LIKE '%Windows%' THEN 'Windows'
      WHEN user_agent LIKE '%Mac OS%' THEN 'macOS'
      WHEN user_agent LIKE '%Android%' THEN 'Android'
      WHEN user_agent LIKE '%iPhone%' OR user_agent LIKE '%iPad%' THEN 'iOS'
      ELSE 'Other'
    END AS operating_system,
  FROM stg_event__filter_checkout
),

final AS (
  SELECT * FROM stg_event__parse_device
)


SELECT * FROM final
