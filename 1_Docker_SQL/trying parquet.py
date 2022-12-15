import pyarrow.parquet as pq
import pandas as pd
trips = pq.read_table('yellow_tripdata_2021-01.parquet')
outcsv = trips.to_pandas()
outcsv.to_csv("Trying CSV.csv",index=False)
print(outcsv.head(10))