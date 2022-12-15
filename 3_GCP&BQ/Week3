google to load stored data to BigQuery
airflow.providers.google.cloud.transfers.gcs_to_gcs
airflow.providers.google.cloud.operators.bigquery =>> BigQueryCreateExternalTableOperator, externalDataConfiguration, BigQueryInsertJobOperator


# SQL used in Big Query for homework:

### Question 1: 
#**What is count for fhv vehicles data for year 2019**  
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_de-zoom-359609/raw/fhv_out/fhv_out_2019-*.parquet']
);
SELECT count(*) FROM `trips_data_all.fhv_tripdata`;
### Question 2: 
#**How many distinct dispatching_base_num we have in fhv for 2019**  
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `trips_data_all.fhv_tripdata`;

### Question 3: 
#**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
CREATE OR REPLACE TABLE `trips_data_all.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `trips_data_all.fhv_tripdata`;

### Question 4: 
#**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
CREATE OR REPLACE TABLE `trips_data_all.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `trips_data_all.fhv_tripdata`
);

### Question 5: 
#**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
SELECT count(*) FROM  `trips_data_all.fhv_nonpartitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');

### Question 6: 
#**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
SELECT count(*) FROM `trips_data_all.fhv_partitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
  
### (Not required) Question 7: 
**In which format does BigQuery save data**  
Column-oriented format
