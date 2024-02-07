## Week 3 Homework
ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402
- 1,936,423
- 253,647

<b>Answer Question 1 : 840,402</b>
</br><b>Answer Query : SELECT COUNT(*) AS total FROM taxi_rides_ny.green_tripdata_non_partitoned;</b></br>

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

<b>Answer Question 2 : 0 MB for the External Table and 6.41MB for the Materialized Table</b>
</br><b>Answer Query : 

External Table : SELECT DISTINCT(PULocationID) FROM taxi_rides_ny.external_green_tripdata;

Materialized Table : SELECT DISTINCT(PULocationID)
FROM taxi_rides_ny.green_tripdata_non_partitoned;</b></br>


## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622

<b>Answer Question 3 : 1,622</b>
</br><b>Answer Query : SELECT COUNT(*) AS fare_amount_of_0 FROM taxi_rides_nygreen_tripdata_non_partitoned WHERE fare_amount = 0.0;</b></br>

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

<b>Answer Question 4 : Partition by lpep_pickup_datetime Cluster on PUlocationID</b>
</br><b>Answer Query : CREATE OR REPLACE TABLE taxi_rides_ny.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM taxi_rides_ny.external_green_tripdata;</b></br>

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

<b>Answer Question 5 : 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table</b>
</br><b>Answer Query :

Non-Partitioned Table : SELECT DISTINCT(PULocationID)
FROM taxi_rides_ny.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

Partitioned Table : SELECT DISTINCT(PULocationID)
FROM taxi_rides_ny.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';</b></br>

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

<b>Answer Question 6 : GCP Bucket</b>


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

<b>Answer Question 7 : False</b>

## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

<b>Answer Question 8 :  estimate the number of bytes that will be read in BigQuery for a SELECT COUNT(*) query on a materialized table, you need to consider the size of the table and the columns involved in the query.
The estimated bytes read for this query depends on the size of the table and the number of columns. In BigQuery, when you execute a SELECT COUNT(*) query, it reads only the metadata, not the actual data. The metadata includes information about the table structure, such as column names, data types, and other properties.</b>

 
## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3


