import pandas as pd
df = pd.read_csv('taxi+_zone_lookup.csv')

from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df.to_sql(name='zones',con=engine,if_exists='replace')
print("Done uploading Zones")