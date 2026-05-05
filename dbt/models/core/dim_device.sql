{{
  config(
    materialized='table',
    schema='core'
  )
}}

WITH
unknown_members AS (
  SELECT
    -1 AS device_key,
    'Unknown' AS device_category,
    'Unknown' AS browser,
    'Unknown' AS os,
    FALSE AS is_mobile

  UNION ALL

  SELECT
    -2 AS device_key,
    'N/A' AS device_category,
    'N/A' AS browser,
    'N/A' AS os,
    FALSE AS is_mobile
),

int_event__get_distinct_device AS (
  SELECT DISTINCT
    device_category,
    browser,
    os
  FROM {{ ref('int_events_checkout_success') }}
),

int_event__add_surrogate_key AS (
  SELECT
    -- Surrogate key (start from 1 to avoid collision with Unknown rows)
    ROW_NUMBER() OVER (ORDER BY device_category, browser, os) AS device_key,

    -- Attributes
    device_category,
    browser,
    os,

    -- Derived attribute
    CASE
      WHEN device_category IN ('Mobile', 'Tablet') THEN TRUE
      ELSE FALSE
    END AS is_mobile

  FROM int_event__get_distinct_device
),

final AS (
  SELECT * FROM unknown_members
  UNION ALL
  SELECT * FROM int_event__add_surrogate_key
)

SELECT * FROM final
