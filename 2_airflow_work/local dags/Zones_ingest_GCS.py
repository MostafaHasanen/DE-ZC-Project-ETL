from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import logging

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow.utils.dates import days_ago

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
FILE_TEMPLATE = 'taxi+_zone_lookup'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/'+ FILE_TEMPLATE
TABLE_NAME_TEMPLATE = 'Zones_airflow'

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(**kwargs):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(kwargs["bucket"])

    blob = bucket.blob(kwargs["object_name"])
    blob.upload_from_filename(kwargs["local_file"])

local_workflow = DAG(
    "ZonesIngestionDag",
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
)

with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        #To make it fail on 404, add the `-sSLf` flag: data not exist
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}.csv'
    )
    rm_data = BashOperator(
        task_id='rm',
        bash_command=f'rm {OUTPUT_FILE_TEMPLATE}.parquet {OUTPUT_FILE_TEMPLATE}.csv'
    )
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{FILE_TEMPLATE}.parquet",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}.parquet",
        },
    )
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{OUTPUT_FILE_TEMPLATE}.csv",
        },
    )
    wget_task >> format_to_parquet_task >> local_to_gcs_task >> rm_data