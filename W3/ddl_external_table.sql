CREATE OR REPLACE EXTERNAL TABLE `taxi_rides_ny.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://2022_green_taxi_trip_record/*']
);
