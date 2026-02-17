/* @bruin

# Reports: daily aggregates by taxi_type and payment_type for dashboards/analytics.
# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

columns:
  - name: trip_date
    type: date
    description: Date of trips (from pickup_datetime)
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Taxi type (yellow, green)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment type label from lookup
    primary_key: true
  - name: trip_count
    type: integer
    description: Number of trips
    checks:
      - name: not_null
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: Sum of fare_amount (USD)
    checks:
      - name: non_negative
  - name: total_total_amount
    type: float
    description: Sum of total_amount (USD)
    checks:
      - name: non_negative
  - name: total_trip_distance
    type: float
    description: Sum of trip_distance (miles)
    checks:
      - name: non_negative

@bruin */

-- Aggregate staging trips by date, taxi_type, and payment_type_name.
-- Filter by run window so time_interval only refreshes this range.
SELECT
  CAST(pickup_datetime AS DATE) AS trip_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS total_fare_amount,
  SUM(total_amount) AS total_total_amount,
  SUM(trip_distance) AS total_trip_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type_name