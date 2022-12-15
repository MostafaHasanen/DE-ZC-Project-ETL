from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import logging

import pyarrow.csv as pv
import pyarrow.parquet as pq
from sqlalchemy import create_engine

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

def upload_to_pg(user,password,host,port,db,table_name,filePath):
    # user, password, host, port, database name, table name, url of csv
    print(user," ",password," ",host," ",port," ",db," ",table_name," ",filePath)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    def build_daily_history_table(ticket):
        classname = ticket + "_HistoricDay"
        ticket = type(classname, (Base, HistoricDay), {'__tablename__' : ticket+"_daily_history"})
        ticket.__repr__ =  build_daily_history_table_repr
        return ticket
    build_daily_history_table("test").__table__.create(bind = engine)

    print("Finished ingesting data into the postgres database")

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))



default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}

def General_ingest_local_to_GCS(
    dag,
    URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE,
    TABLE_NAME,
    IT_CSV
):
    with dag:
        wget_task = BashOperator(
            task_id='wget',
            #To make it fail on 404, add the `-sSLf` flag: data not exist
            ### stream the output of curl to GCS
            bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}',
            do_xcom_push=True
        )
        rm_data = BashOperator(
            task_id='rm',
            bash_command=f'rm {OUTPUT_FILE_TEMPLATE}'
        )
        local_to_pg_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_pg,
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
        # Convert if CSV file to Parquet
        if ".csv" in FILE_TEMPLATE:
            wget_task = BashOperator(
                task_id='wget',
                bash_command=f'curl -sSL {URL_TEMPLATE} > {IT_CSV}',
                do_xcom_push=True
            )
            format_to_parquet_task = PythonOperator(
                task_id="format_to_parquet_task",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": f"{IT_CSV}",
                },
            )
            rm_data = BashOperator(
                task_id='rm',
                bash_command=f'rm {IT_CSV} {OUTPUT_FILE_TEMPLATE}'
            )
            wget_task >> format_to_parquet_task >> local_to_pg_task >> rm_data
        else:
            wget_task >> local_to_pg_task >> rm_data

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
# Jinja python in format
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FILE_TEMPLATE = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME +"/"+ FILE_TEMPLATE
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

Yellow_Taxi = DAG(
    "Yellow_Taxi_IngestionDag",
    schedule_interval="0 6 1 * *",
    #schedule_interval="@daily"
    # default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2021,1,1),
    catchup=True,
    max_active_runs=1
)
General_ingest_local_to_GCS(
    dag=Yellow_Taxi,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE=FILE_TEMPLATE,
    TABLE_NAME_TEMPLATE = TABLE_NAME_TEMPLATE
)

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data' 
# Jinja python in format
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FILE_TEMPLATE = 'FHV_OUT_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/' + FILE_TEMPLATE
TABLE_NAME_TEMPLATE = 'FHV_OUT_{{ execution_date.strftime(\'%Y_%m\') }}'
FHVIngestionDag = DAG(
    "FHVIngestionDag",
    schedule_interval="0 6 1 * *",
    #schedule_interval="@daily"
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2020,1,1),
    #catchup=False,
    max_active_runs=1
)
General_ingest_local_to_GCS(
    dag=FHVIngestionDag,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE=FILE_TEMPLATE,
    TABLE_NAME_TEMPLATE = TABLE_NAME_TEMPLATE
)

URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
FILE_TEMPLATE = 'taxi+_zone_lookup.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/'+ FILE_TEMPLATE
IT_CSV = AIRFLOW_HOME + '/'+ 'taxi+_zone_lookup.csv'
TABLE_NAME_TEMPLATE = 'Zones_Airflow'

from airflow.utils.dates import days_ago
ZonesIngestionDag = DAG(
    "ZonesIngestionDag",
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
)
General_ingest_local_to_GCS(
    dag=ZonesIngestionDag,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE=FILE_TEMPLATE,
    TABLE_NAME_TEMPLATE = TABLE_NAME_TEMPLATE,
    IT_CSV=IT_CSV
)
