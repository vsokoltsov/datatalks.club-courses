/* @bruin

# Staging: clean, deduplicate, and enrich NYC taxi trips from ingestion.
# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: Trip start time
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip end time
    primary_key: true
    checks:
      - name: not_null
  - name: PULocationID
    type: integer
    description: Pickup location ID (TLC zone)
    primary_key: true
  - name: DOLocationID
    type: integer
    description: Dropoff location ID (TLC zone)
    primary_key: true
  - name: fare_amount
    type: float
    description: Base fare in USD
    primary_key: true
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: Taxi type (yellow, green)
    checks:
      - name: not_null
  - name: payment_type
    type: integer
    description: Payment type ID
  - name: payment_type_name
    type: string
    description: Payment type label from lookup
  - name: trip_distance
    type: float
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: Total charge in USD
    checks:
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: Staging table must not be empty after run
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM staging.trips
    value: 1

@bruin */

-- Staging: filter to time window, join payment lookup, deduplicate by composite key,
-- and drop invalid rows (null PKs, negative amounts).
WITH windowed AS (
  SELECT
    t.*,
    p.payment_type_name
  FROM ingestion.trips t
  LEFT JOIN ingestion.payment_lookup p
    ON CAST(t.payment_type AS INTEGER) = p.payment_type_id
  WHERE t.pickup_datetime >= '{{ start_datetime }}'
    AND t.pickup_datetime < '{{ end_datetime }}'
    AND t.pickup_datetime IS NOT NULL
    AND t.dropoff_datetime IS NOT NULL
    AND (t.fare_amount IS NULL OR t.fare_amount >= 0)
)
SELECT *
FROM windowed
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, COALESCE(fare_amount, 0)
  ORDER BY extracted_at DESC
) = 1
