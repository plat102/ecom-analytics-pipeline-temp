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
    DATE_TRUNC(date_day, YEAR) AS year_start_date,
    LAST_DAY(date_day, YEAR) AS year_end_date,
    MOD(EXTRACT(YEAR FROM date_day), 4) = 0
      AND (MOD(EXTRACT(YEAR FROM date_day), 100) != 0
        OR MOD(EXTRACT(YEAR FROM date_day), 400) = 0) AS is_leap_year,

    -- Quarter attributes
    EXTRACT(QUARTER FROM date_day) AS quarter,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_day) AS STRING)) AS quarter_name,
    DATE_TRUNC(date_day, QUARTER) AS quarter_start_date,
    LAST_DAY(date_day, QUARTER) AS quarter_end_date,

    -- Month attributes
    EXTRACT(MONTH FROM date_day) AS month,
    FORMAT_DATE('%B', date_day) AS month_name,
    FORMAT_DATE('%b', date_day) AS month_name_short,
    DATE_TRUNC(date_day, MONTH) AS month_start_date,
    LAST_DAY(date_day, MONTH) AS month_end_date,
    EXTRACT(DAY FROM LAST_DAY(date_day, MONTH)) AS days_in_month,
    date_day = LAST_DAY(date_day, MONTH) AS is_last_day_of_month,

    -- Week attributes
    EXTRACT(WEEK FROM date_day) AS week,
    EXTRACT(ISOWEEK FROM date_day) AS iso_week,
    EXTRACT(ISOYEAR FROM date_day) AS iso_year,
    DATE_TRUNC(date_day, WEEK(MONDAY)) AS week_start_date,
    DATE_ADD(DATE_TRUNC(date_day, WEEK(MONDAY)), INTERVAL 6 DAY) AS week_end_date,
    CEILING(EXTRACT(DAY FROM date_day) / 7.0) AS week_of_month,

    -- Day attributes
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%a', date_day) AS day_name_short,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,

    -- Boolean flags
    EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) AS is_weekend,
    EXTRACT(DAYOFWEEK FROM date_day) NOT IN (1, 7) AS is_weekday,

    -- Current date flags (for relative date filtering)
    date_day = CURRENT_DATE() AS is_current_day,
    DATE_TRUNC(date_day, MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
    DATE_TRUNC(date_day, QUARTER) = DATE_TRUNC(CURRENT_DATE(), QUARTER) AS is_current_quarter,
    DATE_TRUNC(date_day, YEAR) = DATE_TRUNC(CURRENT_DATE(), YEAR) AS is_current_year,

  FROM date_spine
),

final AS (
  SELECT * FROM dates_enriched
)


SELECT * FROM final
