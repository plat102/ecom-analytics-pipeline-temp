{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH raw_event__deduplicate AS (
  SELECT *
  FROM {{ source('glamira_raw', 'events') }}
{#  WHERE store_id BETWEEN '1' AND '86'  -- Filter invalid store_ids #}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _id
    ORDER BY ingested_at DESC
  ) = 1  -- Keep latest ingested record per event
),

raw_event__rename AS (
  SELECT
    -- Primary key
    _id AS event_id,

    -- Temporal fields (keep original for now)
    time_stamp,

    -- Event metadata
    collection AS event_type,
    store_id,

    -- User identifiers
    user_id_db,
    device_id,
    email_address,

    -- Session context
    ip AS ip_address,
    user_agent,
    resolution,
    current_url,
    referrer_url,

    -- Order fields
    order_id,
    cart_products,

    -- Product interaction
    product_id,

    -- Currency fields
    currency AS currency_code,
    price,

    -- Recommendation context
    recommendation AS is_recommendation_influenced,

    -- Metadata
    ingested_at

  FROM raw_event__deduplicate
),

raw_event__cast_type AS (
  SELECT
    -- Primary key (already correct type)
    event_id,

    -- Temporal fields: convert unix timestamp to TIMESTAMP and DATE
    TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)) AS event_timestamp,
    DATE(TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS event_date,

    -- Event metadata: cast to proper types
    event_type,
    CAST(store_id AS INT64) AS store_id,

    -- User identifiers (keep as STRING)
    user_id_db,
    device_id,
    email_address,

    -- Session context
    ip_address,
    user_agent,
    resolution,
    current_url,
    referrer_url,

    -- Order fields
    order_id,
    cart_products,

    -- Product interaction
    product_id,

    -- Currency fields
    currency_code,
    SAFE_CAST(price AS NUMERIC) AS price,

    -- Recommendation (already BOOL)
    is_recommendation_influenced,

    -- Metadata
    ingested_at

  FROM raw_event__rename
),

raw_event__handle_null AS (
  SELECT
    -- Primary key
    event_id,

    -- Temporal fields
    event_timestamp,
    event_date,

    -- Event metadata
    NULLIF(event_type, '') AS event_type,
    store_id,

    -- User identifiers
    NULLIF(user_id_db, '') AS user_id_db,
    NULLIF(device_id, '') AS device_id,
    NULLIF(email_address, '') AS email_address,

    -- Session context
    NULLIF(ip_address, '') AS ip_address,
    NULLIF(user_agent, '') AS user_agent,
    NULLIF(resolution, '') AS resolution,
    NULLIF(current_url, '') AS current_url,
    NULLIF(referrer_url, '') AS referrer_url,

    -- Order fields
    NULLIF(order_id, '') AS order_id,
    cart_products,

    -- Product interaction
    NULLIF(product_id, '') AS product_id,

    -- Currency fields
    NULLIF(currency_code, '') AS currency_code,
    price,

    -- Recommendation
    is_recommendation_influenced,

    -- Metadata
    ingested_at

  FROM raw_event__cast_type
),

raw_event__add_placeholder AS (
  SELECT
    *,
    -- Placeholder for device fields (to be enriched later in intermediate layer)
    CAST(NULL AS STRING) AS device_category,
    CAST(NULL AS STRING) AS browser_name,
    CAST(NULL AS STRING) AS operating_system

  FROM raw_event__handle_null
),

final AS (
  SELECT * FROM raw_event__add_placeholder
)


SELECT * FROM final
