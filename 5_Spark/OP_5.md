Install Spark using setup folder: 

installing on my windows, and on VM of gcp (linux).
    -java
    -Hadoop binaries
    -Spark
    add paths to .bashrc

Move to Pyspark: add the paths to your .bashrc

while forward path using ssh in visual code, start your notebook in specified folder.
Notebook default at: 8888
Spark master is at port 4040

Going to use high volume data (higher than used before) "Spark should manage even much more higher"
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

- Partition files for Spark clusters job  ### You can read parquet files combined in same df by specify folder only

Explore Spark: and its UI (DAG Visual, Tasks schedules etc...)
- Actions vs Transformations (active vs lazy)
- Download data with parquet format (read all related data together to unit the schema or predefine schema)
# Tree command in linux to take a look at download_data.sh output with related look
sudo apt-get install tree

integerate dbt sql code into spark session code

Internal core (Spark UI explore/understand):
    Spark Clusters: Master (entry point with UI at 4040) ==>> Executors, DFs (Hadoop/HDFS with data lakes stored on executors) "Download code on machine that have the data" // With cloud data center Hadoop and HDFS can not be used as data already exist in same place of execution
    Spark GroupBy, Reshuffling (Exchange)
    Spark JOIN, (Broadcast if small data joins)
    External Merge Sort

Data frames are built on RDDs (Resilient Distributed Datasets)
RDDs and what/how they are built and used, GROUPBY: filter/map/reduce, MapPartition (Best usage of mem. in ML for example)

###
Uploading data to GCS: 
# -m: use all cpu to process
gsutil -m cp -r ~/code/data/files/ gs://dtc_data_lake_de-zoom-359609/FromSpark

Connecting Spark, Hadoop to Cloud Storage
# jar for connecting to GCS: link provided from https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage
.. Change Hadoop version as required
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar


Start Spark cluster locally "stand-alone":https://spark.apache.org/docs/latest/spark-standalone.html
Master:
./sbin/start-master.sh --webui-port PORT ::: Starts on port 

Creating a worker:
URL="spark://{GIVEN in UI}.internal:7077"
./sbin/start-worker.sh ${URL}


Convert notebook to script:
jupyter nbconvert --to=script 2_SQL_Spark.ipynb

Use `spark-submit` for running the script on the cluster

spark-submit \
    --master="${URL}" \
    2_SQL_Spark.py \
        --folder_green=data/files/green/2020/ \
        --folder_yellow=data/files/yellow/2020/ \
        --output=data/report/2020
### Do NOT forget to stop workers and Master

# Dataproc on GCP
Create Cluster on Dataproc in gcp, (create cluster with docker and jupyter nb)

Upload your script to a lake (e.g. file: SQL_Spark_BQ.py)

gsutil cp 2_SQL_Spark-BQ.py gs://dtc_data_lake_de-zoom-359609/code/Spark_Sql.py

submit job on your dataproc: with the specified script
args:
* `--folder_green=gs://dtc_data_lake_de-zoom-359609/FromSpark/green/2020/`
* `--folder_yellow=gs://dtc_data_lake_de-zoom-359609/FromSpark/yellow/2020/`
* `--output=gs://dtc_data_lake_de-zoom-359609/report/2020`
Insert your args to your job while creating

you can submit your job using REST api: https://cloud.google.com/dataproc/docs/guides/submit-job
# Add authorization to account to submit Dataproc jobs
gcloud dataproc jobs submit job-command \
    --cluster=cluster-name \
    --region=region \
    other dataproc-flags \
    -- job-args

gcloud dataproc jobs submit pyspark \
    --cluster=spark-sql-cluster \
    --region=europe-west1 \
    gs://dtc_data_lake_de-zoom-359609/code/Spark_Sql.py \
    -- \
        --folder_green=gs://dtc_data_lake_de-zoom-359609/FromSpark/green/2020/ \
        --folder_yellow=gs://dtc_data_lake_de-zoom-359609/FromSpark/yellow/2020/ \
        --output=gs://dtc_data_lake_de-zoom-359609/report/2020
