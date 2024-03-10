/* Q.0*/

CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

/* Q.1*/

CREATE MATERIALIZED VIEW trip_describe AS
SELECT
  taxi_zone_pu.Zone as pickup_zone,
  taxi_zone_do.Zone as dropoff_zone,
  count(*) as number_of_trips,
  avg(tpep_dropoff_datetime - tpep_pickup_datetime) as avg_trip_time,
  min(tpep_dropoff_datetime - tpep_pickup_datetime) as min_trip_time,
  max(tpep_dropoff_datetime - tpep_pickup_datetime) as max_trip_time
FROM trip_data
JOIN taxi_zone as taxi_zone_pu
  ON trip_data.PULocationID = taxi_zone_pu.location_id
JOIN taxi_zone as taxi_zone_do
  ON trip_data.DOLocationID = taxi_zone_do.location_id
GROUP BY pickup_zone, dropoff_zone;

----------------------------------------------------------------------
CREATE MATERIALIZED VIEW avg_trip_time_max AS
WITH highest_average_trip_time AS (
  SELECT max(avg_trip_time) AS max
  FROM trip_describe
)
SELECT
  pickup_zone,
  dropoff_zone,
  number_of_trips,
  avg_trip_time
FROM trip_describe, highest_average_trip_time
WHERE avg_trip_time = max;

----------------------------------------------------------------------
dev=> SELECT * FROM avg_trip_time_max;
  pickup_zone   | dropoff_zone | number_of_trips | avg_trip_time
----------------+--------------+-----------------+---------------
 Yorkville East | Steinway     |               1 | 23:59:33
(1 row)

----------------------------------------------------------------------

/* Q.2*/

CREATE MATERIALIZED VIEW avg_trip_time_max AS
WITH highest_average_trip_time AS (
  SELECT max(avg_trip_time) AS max
  FROM trip_describe
)
SELECT
  pickup_zone,
  dropoff_zone,
  number_of_trips,
  avg_trip_time
FROM trip_describe, highest_average_trip_time
WHERE avg_trip_time = max;

----------------------------------------------------------------------
dev=> SELECT * FROM avg_trip_time_max;
  pickup_zone   | dropoff_zone | number_of_trips | avg_trip_time
----------------+--------------+-----------------+---------------
 Yorkville East | Steinway     |               1 | 23:59:33
(1 row)

----------------------------------------------------------------------

/* Q.3*/

CREATE MATERIALIZED VIEW busiest_pickups AS
WITH latest_pickup_time AS (
  SELECT max(tpep_pickup_datetime) AS max
  FROM trip_data
)
SELECT
  taxi_zone.Zone as pickup_zone,
  count(*)
FROM latest_pickup_time,
     trip_data
JOIN taxi_zone
  ON trip_data.PULocationID = taxi_zone.location_id
WHERE tpep_pickup_datetime >= max - interval '17 hours'
GROUP BY pickup_zone;

----------------------------------------------------------------------
dev=> SELECT * FROM busiest_pickups ORDER BY count DESC LIMIT 3;
     pickup_zone     | count
---------------------+-------
 LaGuardia Airport   |    19
 Lincoln Square East |    17
 JFK Airport         |    17
(3 rows)
