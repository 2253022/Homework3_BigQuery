-- Homework 3: Data Warehousing & BigQuery
-- Author: Lefalamang Koaleli
-- Date: 2026-02-07
-- Description: All queries for Questions 1-9 in a single SQL file

CREATE OR REPLACE EXTERNAL TABLE `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024_ext`
OPTIONS (
  format='PARQUET',
  uris=[
    'gs://koaleli-nyc-taxi-2024/yellow/yellow_tripdata_2024-01.parquet',
    'gs://koaleli-nyc-taxi-2024/yellow/yellow_tripdata_2024-02.parquet',
    'gs://koaleli-nyc-taxi-2024/yellow/yellow_tripdata_2024-03.parquet',
    'gs://koaleli-nyc-taxi-2024/yellow/yellow_tripdata_2024-04.parquet',
    'gs://koaleli-nyc-taxi-2024/yellow/yellow_tripdata_2024-05.parquet',
    'gs://koaleli-nyc-taxi-2024/yellow/yellow_tripdata_2024-06.parquet'
  ]
);


CREATE OR REPLACE TABLE `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`
AS
SELECT *
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024_ext`;

----------------------------------------
-- Q1 — Total Records
----------------------------------------
-- Count total records for 2024 Yellow Taxi data

SELECT COUNT(*) AS total_records
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`;

--Answer:20,332,093

----------------------------------------
-- Q2 — Distinct PULocationID
----------------------------------------
-- Count distinct PULocationIDs on External Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024_ext`;
--0MB

--Count distinct PULocationIDs on Materialized Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`;
#155.12 MB
-- Estimated bytes:
-- External Table: ~0 MB
-- Materialized Table: ~155.12 MB


----------------------------------------
-- Q3 — Columnar Storage Bytes
----------------------------------------
-- Retrieve 1 column
SELECT PULocationID
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`;

-- Retrieve 2 columns
SELECT PULocationID, DOLocationID
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`;

--Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed


----------------------------------------
-- Q4 — Zero Fare Trips
----------------------------------------
#8333
SELECT COUNT(*) AS zero_fare_trips
FROM (
  SELECT fare_amount
  FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`
  WHERE fare_amount = 0
  LIMIT 1000000
);
-- Answer: 8333

----------------------------------------
-- Q5 — Partition & Cluster Table
----------------------------------------

CREATE OR REPLACE TABLE `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`;

--Answer:Partition by tpep_dropoff_datetime and Cluster on VendorID

----------------------------------------
-- Q6 — Distinct VendorID (Partition Benefit)
----------------------------------------
SELECT DISTINCT VendorID
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Estimated bytes:
-- Non-partitioned table: ~310.24 MB
-- Partitioned table: ~26.84

----------------------------------------
-- Q7 — External Table Storage
----------------------------------------
-- Answer: Data is stored in the GCS bucket
-- gs://koaleli-nyc-taxi-2024/yellow/

----------------------------------------
-- Q8 — Clustering Best Practice
----------------------------------------
-- Answer: True
-- Clustering improves query performance when filtering or ordering by clustered columns.

----------------------------------------
-- Q9 — Count Materialized Table
----------------------------------------
SELECT COUNT(*) AS total_records
FROM `project-63808ccf-cb8f-4204-919.ny_taxi.yellow_taxi_2024`;

-- Estimated bytes read: 0 MB
-- Reason: BigQuery uses table metadata to compute COUNT(*) without scanning full table.





