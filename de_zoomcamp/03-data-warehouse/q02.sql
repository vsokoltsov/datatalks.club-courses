-- This query will process 155.12 MB when run.
select count(distinct PULocationID) from `zoomcamp.yellow_taxi_mv_2024`;
-- This query will process 0 MB when run.
select count(distinct PULocationID) from `zoomcamp.yellow_taxi_trip_records`;