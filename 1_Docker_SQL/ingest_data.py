#!/usr/bin/env python
# coding: utf-8
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
#import pyarrow.parquet as pq
import os

def main(params):
    # user, password, host, port, database name, table name, url of csv
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {'output.parquet'}")
    #outcsv = pq.read_table("output.parquet")
    outcsv = pd.read_parquet("output.parquet")
    #outcsv = outcsv.to_pandas()
    #outcsv.to_csv(csv_name,index=False)
    print("Just overwrite CSV with huge PANDAS DF")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    outcsv[["tpep_pickup_datetime","tpep_dropoff_datetime"]] = outcsv[["tpep_pickup_datetime","tpep_dropoff_datetime"]].apply(pd.to_datetime)
    outcsv.to_sql(name= table_name, con=engine, if_exists='replace')
    print("Finished ingesting data into the postgres database by PANDAS PARQUET")
    #with pd.read_csv(csv_name, chunksize=100000) as reader:
    #    reader.get_chunk(0).head(n=0).to_sql(name= table_name, con=engine, if_exists='replace')
    #    for chunk in reader:
    #        t_start = time()
    #        
    #        chunk[["tpep_pickup_datetime","tpep_dropoff_datetime"]] = chunk[["tpep_pickup_datetime","tpep_dropoff_datetime"]].apply(pd.to_datetime)
    #        chunk.to_sql(name= table_name, con=engine, if_exists='append')
    #
    #        t_end = time()
    #        print('inserted another chunk, took %.3f second' % (t_end - t_start))
    #    print("Finished ingesting data into the postgres database")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
