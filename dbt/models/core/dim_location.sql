{{
  config(
    materialized='table',
    schema='core',
    cluster_by=['country_name', 'region_name']
  )
}}

WITH
unknown_members AS (
  SELECT
    -1 AS location_key,
    'Unknown' AS country_name,
    'Unknown' AS region_name,
    'Unknown' AS city_name,
    -1 AS geo_completeness_level

  UNION ALL

  SELECT
    -2 AS location_key,
    'N/A' AS country_name,
    'N/A' AS region_name,
    'N/A' AS city_name,
    -2 AS geo_completeness_level
),

stg_ip_location__get_distinct AS (
  SELECT DISTINCT
    country_name,
    region_name,
    city_name,
    geo_completeness_level
  FROM {{ ref('stg_ip_locations') }}
  WHERE country_name IS NOT NULL
),

stg_ip_location__add_surrogate_key AS (
  SELECT
    -- Surrogate key (start from 1 to avoid collision with Unknown rows)
    ROW_NUMBER() OVER (ORDER BY country_name, region_name, city_name) AS location_key,

    -- Natural key (composite: country, region, city)
    country_name,
    region_name,
    city_name,

    -- Quality indicator
    geo_completeness_level

  FROM stg_ip_location__get_distinct
),

final AS (
  SELECT * FROM unknown_members
  UNION ALL
  SELECT * FROM stg_ip_location__add_surrogate_key
)

SELECT * FROM final
