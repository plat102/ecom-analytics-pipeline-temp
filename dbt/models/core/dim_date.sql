{{
  config(
    materialized='table',
    schema='core'
  )
}}

WITH date_spine AS (
  SELECT date_day
  FROM UNNEST(
    GENERATE_DATE_ARRAY('2015-01-01', '2030-12-31', INTERVAL 1 DAY)
  ) AS date_day
),

dates_enriched AS (
  SELECT
    -- Surrogate key (YYYYMMDD integer format)
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS date_key,

    -- Natural key
    date_day AS full_date,

    -- Year attributes
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(WEEK FROM date_day) AS week,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,

    -- Derived attributes
    EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) AS is_weekend,

  FROM date_spine
),

final AS (
  SELECT * FROM dates_enriched
)


SELECT * FROM final
