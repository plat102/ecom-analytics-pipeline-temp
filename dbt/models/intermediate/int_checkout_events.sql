{{
  config(
    materialized='view'
  )
}}

WITH events_filtered AS (
  SELECT
    event_id,
    event_timestamp,
    event_date,
    event_type,
    store_id,
    user_id_db,
    device_id,
    ip,
    user_agent,
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

final AS (
  SELECT * FROM events_filtered
)


SELECT * FROM final
