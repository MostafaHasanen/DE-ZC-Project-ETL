from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import logging

from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data' 
# Jinja python in format
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FILE_TEMPLATE = 'FHV_OUT_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/' + FILE_TEMPLATE
TABLE_NAME_TEMPLATE = 'FHV_OUT_{{ execution_date.strftime(\'%Y_%m\') }}'

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

# # No need for: data converted to parquet on site
# def format_to_parquet(src_file):
#     if not src_file.endswith('.csv'):
#         logging.error("Can only accept source files in CSV format, for the moment")
#         return
#     table = pv.read_csv(src_file)
#     pq.write_table(table, src_file.replace('.csv', '.parquet'))

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
    "FHVIngestionDag",
    schedule_interval="0 6 1 * *",
    #schedule_interval="@daily"
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2020,1,1),
    #catchup=False,
    max_active_runs=1
)

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
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{FILE_TEMPLATE}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )
    wget_task >> local_to_gcs_task >> rm_data