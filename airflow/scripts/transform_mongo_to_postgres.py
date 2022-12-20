from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import json
import numpy as np


# init mongoDB client
client = MongoClient("mongodb+srv://dev:dev@cluster0.bul8bqv.mongodb.net/?retryWrites=true&w=majority")
db = client["sample_training"]

# read data
companies = db["companies"].find()
zips = db["zips"].find()

## solution 1
df_companies = pd.DataFrame(companies)
df_zips = pd.DataFrame(zips)

# drop empty column
df_companies = df_companies.dropna(how='all', axis=1)
df_zips = df_zips.dropna(how='all', axis=1)

# transform
df_companies_normalize = pd.DataFrame()
for col in df_companies.columns:
    x = df_companies[col][0]
    if isinstance(x, dict) or isinstance(x, list):
        if col == 'offices':
            offices = df_companies[col].apply(lambda x: np.nan if len(x)==0 else x[0])
            offices = pd.json_normalize(offices)
            offices.columns = ["office_"+x for x in offices.columns]
            df_companies_normalize = pd.concat([df_companies_normalize, offices], axis=1, join="inner")
    else:
        df_companies_normalize[col] = df_companies[col]

zips_loc = pd.json_normalize(df_zips['loc'])
zips_loc = zips_loc.rename({'x':'longitude', 'y':'latitude'}, axis=1)
df_zips_normalize = pd.concat([df_zips, zips_loc], axis=1, join="inner")
df_zips_normalize = df_zips_normalize.drop('loc', axis=1)

df_companies_normalize['_id'] = df_companies_normalize['_id'].astype(str)
df_zips_normalize['_id'] = df_zips_normalize['_id'].astype(str)

# init postgres connection
url = 'postgresql://postgres:anypassword@postgres:5432/postgres'
engine = create_engine(url)

# write to postgres
df_companies_normalize.to_sql('companies', index=False, con=engine, schema='public', if_exists='replace')
df_zips_normalize.to_sql('zips', index=False, con=engine, schema='public', if_exists='replace')
