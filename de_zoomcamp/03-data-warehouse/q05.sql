CREATE OR REPLACE TABLE `zoomcamp.yellow_taxi_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `zoomcamp.yellow_taxi_2024`;