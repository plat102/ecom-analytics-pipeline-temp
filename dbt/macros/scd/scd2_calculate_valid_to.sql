{#
  Calculate valid_to date for SCD Type 2 temporal versioning.

  Logic:
  - Use LEAD() to peek at the next version's timestamp
  - If next version exists: valid_to = start date of next version
  - If no next version (latest version): valid_to = 9999-12-31 (scd_end_date)

  Parameters:
  - timestamp_column: Column containing version start timestamp
  - partition_by_column: Natural key to partition versions (e.g., customer_natural_key)
  - fallback_date: Optional custom end date instead of 9999-12-31

  Example:
    customer_natural_key  | first_seen_with_this_email | valid_to (calculated)
    ---------------------|----------------------------|---------------------
    user123              | 2019-10-01                 | 2019-10-15 (next version starts)
    user123              | 2019-10-15                 | 9999-12-31 (no next version)
#}
{% macro scd2_calculate_valid_to(
    timestamp_column,
    partition_by_column,
    fallback_date=none
) %}
  COALESCE(
    LEAD({{ timestamp_column }}) OVER (
      PARTITION BY {{ partition_by_column }}
      ORDER BY {{ timestamp_column }}
    ),
    {% if fallback_date %}
      {{ fallback_date }}
    {% else %}
      {{ scd_end_date() }}
    {% endif %}
  )
{% endmacro %}
