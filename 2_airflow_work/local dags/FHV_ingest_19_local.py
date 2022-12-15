from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import logging

import pandas as pd
from sqlalchemy import create_engine

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
print(1)
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

print(2)
def ingest_call(user,password,host,port,db,table_name,filePath):
    # user, password, host, port, database name, table name, url of csv
    outparq = pd.read_parquet(filePath)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    #outparq[["tpep_pickup_datetime","tpep_dropoff_datetime"]] = outparq[["tpep_pickup_datetime","tpep_dropoff_datetime"]].apply(pd.to_datetime)
    
    outparq.to_sql(name= table_name, con=engine, if_exists='replace',chunksize=10000)
    
    print("Finished ingesting data into the postgres database by PANDAS PARQUET")



local_workflow = DAG(
    "FHVIngestionDag",
    schedule_interval="0 6 1 * *",
    #schedule_interval="@daily"
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2020,1,1),
    #catchup=False,
    max_active_runs=1
)

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data' 
# Jinja python in format
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/FHV_OUT_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'FHV_OUT_{{ execution_date.strftime(\'%Y_%m\') }}'
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