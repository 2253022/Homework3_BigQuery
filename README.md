# Homework 3: Data Warehousing & BigQuery

**Author:** Lefalamang Koaleli  
**Date:** 2026-02-07  
**Course:** Data Engineering Zoomcamp 2026  
**Description:** All queries for Questions 1–9 for Module 3: Data Warehousing & BigQuery.  

All queries are in the single file: `homework3_queries.sql`

---

## Q1 — Total Records
**Description:** Count total records for 2024 Yellow Taxi data.  
**Answer:** 20,332,093  

---

## Q2 — Distinct PULocationID
**Description:** Count distinct PULocationIDs on external and materialized tables.  
**Answer:**  
- External Table: 0 MB (estimated bytes)  
- Materialized Table: 155.12 MB  

---

## Q3 — Columnar Storage Bytes
**Description:** Compare bytes processed when selecting 1 column vs 2 columns.  
**Answer:** BigQuery is columnar; querying 2 columns (PULocationID + DOLocationID) requires reading more data → higher bytes processed.  

---

## Q4 — Zero Fare Trips
**Description:** Count trips with `fare_amount = 0`.  
**Answer:** 8,333  

---

## Q5 — Partition & Cluster Table
**Description:** Create a partitioned table by `tpep_dropoff_datetime` and cluster by `VendorID`.  
**Answer:** Partition by `tpep_dropoff_datetime`, Cluster by `VendorID`  

---

## Q6 — Distinct VendorID (Partition Benefit)
**Description:** Retrieve distinct VendorIDs between 2024-03-01 and 2024-03-15; compare estimated bytes.  
**Answer:**  
- Non-partitioned table: ~310.24 MB  
- Partitioned table: ~26.84 MB  

---

## Q7 — External Table Storage
**Description:** Where is the data stored for the external table?  
**Answer:** GCS bucket: `gs://koaleli-nyc-taxi-2024/yellow/`  

---

## Q8 — Clustering Best Practice
**Description:** Is it best practice to always cluster data in BigQuery?  
**Answer:** True — improves query performance for filters and ordering.  

---

## Q9 — Count Materialized Table
**Description:** Count total rows from the materialized table; explain estimated bytes read.  
**Answer:**  
- Estimated bytes read: 0 MB  
- Reason: BigQuery uses table metadata to compute `COUNT(*)` without scanning full table.  

---

## Notes
- All SQL queries use **project `project-63808ccf-cb8f-4204-919`** and dataset `ny_taxi`.  
- Data covers **January–June 2024**.  
- Materialized tables improve query performance over external tables.  
- Partitioning and clustering reduce bytes processed and make queries faster.
