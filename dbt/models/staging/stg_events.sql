{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH events_deduped AS (
  SELECT *
  FROM {{ source('glamira_raw', 'events') }}
  WHERE store_id BETWEEN '1' AND '86'  -- Filter invalid store_ids
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _id
    ORDER BY ingested_at DESC
  ) = 1  -- Keep latest ingested record per event
),

events_cleaned AS (
  SELECT
    -- Primary key
    _id AS event_id,

    -- Temporal fields (time_stamp is INTEGER unix timestamp)
    TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)) AS event_timestamp,
    DATE(TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS event_date,

    -- Event metadata (collection is the event type)
    NULLIF(collection, '') AS event_type,
    CAST(store_id AS INT64) AS store_id,

    -- User identifiers
    NULLIF(user_id_db, '') AS user_id_db,
    NULLIF(device_id, '') AS device_id,
    NULLIF(email_address, '') AS email_address,

    -- Session context
    NULLIF(ip, '') AS ip,

    -- Device/browser fields - Note: These may need to be parsed from user_agent
    NULLIF(user_agent, '') AS user_agent,
    NULLIF(resolution, '') AS resolution,

    -- Placeholder for device fields (to be enriched later if needed)
    CAST(NULL AS STRING) AS device_category,
    CAST(NULL AS STRING) AS browser,
    CAST(NULL AS STRING) AS os,

    -- Order fields (for checkout_success events)
    NULLIF(order_id, '') AS order_id,
    cart_products,  -- ARRAY<STRUCT>

    -- Product interaction
    NULLIF(product_id, '') AS product_id,

    -- Currency fields
    NULLIF(currency, '') AS currency_code,
    SAFE_CAST(price AS NUMERIC) AS price,

    -- Recommendation context (field is already BOOL)
    recommendation AS is_recommendation_influenced,

    -- Metadata
    ingested_at,

  FROM events_deduped
),

final AS (
  SELECT * FROM events_cleaned
)


SELECT * FROM final
