-- This query will process 310.24 MB when run.
select distinct VendorID from `zoomcamp.yellow_taxi_2024` 
where tpep_dropoff_datetime > '2024-03-01' and 
tpep_dropoff_datetime <= '2024-03-15';
-- This query will process 26.84 MB when run.
select distinct VendorID from `zoomcamp.yellow_taxi_2024_optimized` 
where tpep_dropoff_datetime > '2024-03-01' and 
tpep_dropoff_datetime <= '2024-03-15';