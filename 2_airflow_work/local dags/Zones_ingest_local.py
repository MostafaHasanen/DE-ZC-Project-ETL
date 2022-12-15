from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago

import logging
import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.csv as pv
import pyarrow.parquet as pq
def ingest_call(user,password,host,port,db,table_name,filePath):
    # user, password, host, port, database name, table name, url of csv
    outparq = pd.read_parquet(filePath)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    outparq.to_sql(name= table_name, con=engine, if_exists='replace')
    print("Finished ingesting data into the postgres database by PANDAS PARQUET")
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

local_workflow = DAG(
    "ZonesIngestionDag",
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
)

URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
# Jinja python in format
Output_file = AIRFLOW_HOME + '/taxi+_zone_lookup'
TABLE_NAME_TEMPLATE = 'Zones_airflow'

with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        #To make it fail on 404, add the `-sSLf` flag: data not exist
        bash_command=f'curl -sSL {URL} > {Output_file}.csv'
    )
    rm_data = BashOperator(
        task_id='rm',
        bash_command=f'rm {Output_file}.parquet {Output_file}.csv'
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
            filePath=f'{Output_file}.parquet'
        ),
    )
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{Output_file}.csv",
        },
    )
    wget_task >> format_to_parquet_task >> ingest_task >> rm_data