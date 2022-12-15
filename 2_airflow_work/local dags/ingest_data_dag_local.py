from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import logging

import pandas as pd
import numpy as np
import pyarrow.parquet as pq

from sqlalchemy import create_engine

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

def ingest_call(user,password,host,port,db,table_name,filePath):
    # user, password, host, port, database name, table name, url of csv
    print(user," ",password," ",host," ",port," ",db," ",table_name," ",filePath)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print("engine created")
    pq.read_table(filePath).to_pandas().to_sql(name=table_name, con=engine, if_exists='replace')
    print("Any access to dataframe break me")
    #outparq[["tpep_pickup_datetime","tpep_dropoff_datetime"]] = outparq[["tpep_pickup_datetime","tpep_dropoff_datetime"]].apply(pd.to_datetime)
    print("To SQL BREAK ME")
    # outparq.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    # print("table created")
    # outparq.groupby(np.arrange(len(outparq))//1000000)
    # for k,g in outparq.groupby(np.arrange(len(outparq))//1000000):
    #     print(k,g)
    #     break
        #g.to_sql(name= table_name, con=engine, if_exists='append')
    print("Finished ingesting data into the postgres database by PANDAS PARQUET")

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 1 * *",
    #schedule_interval="@daily"
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2021,1,1),
    #catchup=False,
    max_active_runs=1
)

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data' 
# Jinja python in format
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        #To make it fail on 404, add the `-sSLf` flag: data not exist
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )
    rm_data = BashOperator(
        task_id='rm',
        bash_command=f'rm {OUTPUT_FILE_TEMPLATE}'
    )
    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_call,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            filePath=OUTPUT_FILE_TEMPLATE
        ),
    )
    wget_task >> ingest_task >> rm_data
    
    # # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    # local_to_gcs_task = PythonOperator(
    #     task_id="local_to_gcs_task",
    #     python_callable=upload_to_gcs,
    #     op_kwargs={
    #         "bucket": BUCKET,
    #         "object_name": f"raw/{parquet_file}",
    #         "local_file": f"{path_to_local_home}/{parquet_file}",
    #     },
    # )

# # NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
# def upload_to_gcs(bucket, object_name, local_file):
#     """
#     Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
#     :param bucket: GCS bucket name
#     :param object_name: target path & file-name
#     :param local_file: source path & file-name
#     :return:
#     """
#     # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
#     # (Ref: https://github.com/googleapis/python-storage/issues/74)
#     storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
#     storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
#     # End of Workaround

#     client = storage.Client()
#     bucket = client.bucket(bucket)

#     blob = bucket.blob(object_name)
#     blob.upload_from_filename(local_file)

