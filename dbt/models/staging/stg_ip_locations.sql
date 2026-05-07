{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH raw_ip_location__deduplicate AS (
  SELECT *
  FROM {{ source('glamira_raw', 'ip_locations') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ip ORDER BY ingested_at DESC) = 1
),

raw_ip_location__rename AS (
  SELECT
    -- Primary key
    ip AS ip_address,

    -- Geographic fields
    country AS country_name,
    region AS region_name,
    city AS city_name,

    -- Metadata
    ingested_at

  FROM raw_ip_location__deduplicate
),

raw_ip_location__handle_null AS (
  SELECT
    -- Primary key
    ip_address,

    -- Geographic fields: convert empty strings to NULL
    NULLIF(country_name, '') AS country_name,
    NULLIF(region_name, '') AS region_name,
    NULLIF(city_name, '') AS city_name,

    -- Metadata
    ingested_at

  FROM raw_ip_location__rename
),

raw_ip_location__add_derived_field AS (
  SELECT
    *,

    -- Derived field: Geo completeness level
    CASE
      WHEN city_name IS NOT NULL THEN 3  -- Full geo data
      WHEN region_name IS NOT NULL THEN 2  -- Partial (country + region)
      WHEN country_name IS NOT NULL THEN 1  -- Minimal (country only)
      ELSE 0  -- No geo data
    END AS geo_completeness_level

  FROM raw_ip_location__handle_null
),

final AS (
  SELECT * FROM raw_ip_location__add_derived_field
)


SELECT * FROM final
