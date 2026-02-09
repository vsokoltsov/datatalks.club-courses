CREATE OR REPLACE EXTERNAL TABLE `zoomcamp.yellow_taxi_trip_records`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://vs_dezoomcamp_hw3_2025/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `zoomcamp.yellow_taxi_2024`
AS
SELECT *
FROM `zoomcamp.yellow_taxi_trip_records`;

CREATE OR REPLACE MATERIALIZED VIEW `zoomcamp.yellow_taxi_mv_2024` AS
SELECT *
FROM `zoomcamp.yellow_taxi_2024`;
