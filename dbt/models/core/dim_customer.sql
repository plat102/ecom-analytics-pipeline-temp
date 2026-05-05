-- SCD Type 2 implementation with temporal backfill capability
-- Tracks email_address changes over time
-- Initial load: Reconstructs 826 historical versions from Oct-Nov 2019 events
-- Incremental runs: Detects changes via row_hash and maintains version history
{{
  config(
    materialized='incremental',
    unique_key='customer_key',
    schema='core',
    cluster_by=['customer_natural_key', 'is_current'],
    on_schema_change='append_new_columns'
  )
}}

WITH
unknown_members AS (
  SELECT
    -1 AS customer_key,
    'UNKNOWN' AS customer_natural_key,
    CAST(NULL AS STRING) AS user_id_db,
    CAST(NULL AS STRING) AS device_id,
    CAST(NULL AS STRING) AS email_address,
    TIMESTAMP('1900-01-01 00:00:00') AS valid_from,
    {{ scd_end_date() }} AS valid_to,
    TRUE AS is_current,
    {{ scd2_row_hash(['CAST(NULL AS STRING)']) }} AS row_hash

  UNION ALL

  SELECT
    -2 AS customer_key,
    'N/A' AS customer_natural_key,
    CAST(NULL AS STRING) AS user_id_db,
    CAST(NULL AS STRING) AS device_id,
    CAST(NULL AS STRING) AS email_address,
    TIMESTAMP('1900-01-01 00:00:00') AS valid_from,
    {{ scd_end_date() }} AS valid_to,
    TRUE AS is_current,
    {{ scd2_row_hash(['CAST(NULL AS STRING)']) }} AS row_hash
),

stg_event__extract_user AS (
  SELECT
    COALESCE(user_id_db, device_id) AS customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    event_timestamp
  FROM {{ ref('stg_events') }}
  WHERE COALESCE(user_id_db, device_id) IS NOT NULL
),

stg_event__track_email_change AS (
  SELECT
    customer_natural_key,
    MAX(user_id_db) AS user_id_db,
    MAX(device_id) AS device_id,
    email_address,
    MIN(event_timestamp) AS first_seen_with_this_email,
    MAX(event_timestamp) AS last_seen_with_this_email,
    ROW_NUMBER() OVER (
      PARTITION BY customer_natural_key
      ORDER BY MIN(event_timestamp)
    ) AS version_number,
    {{ scd2_row_hash(['email_address']) }} AS row_hash
  FROM stg_event__extract_user
  GROUP BY
    customer_natural_key,
    email_address
),

stg_event__prepare_staging AS (
  SELECT
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    first_seen_with_this_email,
    version_number,
    row_hash,
    CURRENT_DATE() AS snapshot_date
  FROM stg_event__track_email_change
),

{% if is_incremental() %}

dim_customer__get_current AS (
  SELECT *
  FROM {{ this }}
  WHERE is_current = TRUE
),

dim_customer__detect_change AS (
  SELECT
    s.customer_natural_key,
    e.customer_key AS old_customer_key,
    s.user_id_db,
    s.device_id,
    s.email_address,
    s.row_hash,
    s.snapshot_date
  FROM stg_event__prepare_staging s
  INNER JOIN dim_customer__get_current e
    ON s.customer_natural_key = e.customer_natural_key
  WHERE s.row_hash != e.row_hash
),

dim_customer__expire_old_version AS (
  SELECT
    customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    valid_from,
    CURRENT_TIMESTAMP() AS valid_to,
    FALSE AS is_current,
    row_hash
  FROM {{ this }}
  WHERE customer_natural_key IN (
    SELECT customer_natural_key FROM dim_customer__detect_change
  )
  AND is_current = TRUE
),

dim_customer__create_new_version AS (
  SELECT
    {{ generate_incremental_surrogate_key('customer_natural_key',
      key_column='customer_key') }} AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    CURRENT_TIMESTAMP() AS valid_from,
    {{ scd_end_date() }} AS valid_to,
    TRUE AS is_current,
    row_hash
  FROM dim_customer__detect_change
),

dim_customer__identify_new AS (
  SELECT
    s.customer_natural_key,
    s.user_id_db,
    s.device_id,
    s.email_address,
    s.row_hash
  FROM stg_event__prepare_staging s
  LEFT JOIN dim_customer__get_current e
    ON s.customer_natural_key = e.customer_natural_key
  WHERE e.customer_natural_key IS NULL
),

dim_customer__insert_new AS (
  SELECT
    {{ generate_incremental_surrogate_key('customer_natural_key', key_column='customer_key') }} AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    CURRENT_TIMESTAMP() AS valid_from,
    {{ scd_end_date() }} AS valid_to,
    TRUE AS is_current,
    row_hash
  FROM dim_customer__identify_new
),

{% else %}

stg_event__reconstruct_version AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY customer_natural_key, version_number) AS customer_key,
    customer_natural_key,
    user_id_db,
    device_id,
    email_address,
    first_seen_with_this_email AS valid_from,
    {{ scd2_calculate_valid_to('first_seen_with_this_email', 'customer_natural_key') }} AS valid_to,
    {{ scd2_calculate_is_current('first_seen_with_this_email', 'customer_natural_key') }} AS is_current,
    row_hash
  FROM stg_event__prepare_staging
),

{% endif %}

final AS (
  SELECT * FROM unknown_members
  UNION ALL
  {% if is_incremental() %}
    SELECT * FROM dim_customer__expire_old_version
    UNION ALL
    SELECT * FROM dim_customer__create_new_version
    UNION ALL
    SELECT * FROM dim_customer__insert_new
  {% else %}
    SELECT * FROM stg_event__reconstruct_version
  {% endif %}
)

SELECT * FROM final
