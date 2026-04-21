{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH locations_cleaned AS (
  SELECT
    -- Primary key
    ip,

    -- Geographic fields (rename to match design)
    NULLIF(country, '') AS country_name,
    NULLIF(region, '') AS region_name,
    NULLIF(city, '') AS city_name,

    -- Derived fields: Geo completeness level
    CASE
      WHEN city IS NOT NULL AND city != ''
      THEN 3  -- Full geo data (country + region + city)

      WHEN region IS NOT NULL AND region != ''
      THEN 2  -- Partial (country + region)

      WHEN country IS NOT NULL AND country != ''
      THEN 1  -- Minimal (country only)

      ELSE 0  -- No geo data
    END AS geo_completeness_level,

    -- Boolean flag for filtering
    CASE
      WHEN country IS NOT NULL AND country != ''
      THEN TRUE
      ELSE FALSE
    END AS has_geo_data,

    -- Metadata
    ingested_at,

  FROM {{ source('glamira_raw', 'ip_locations') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ip ORDER BY ingested_at DESC) = 1
),

final AS (
  SELECT * FROM locations_cleaned
)


SELECT * FROM final
