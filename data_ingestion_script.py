import pandas as pd
from sqlalchemy import create_engine
import argparse

def ingest_data(params):

    
    user = params.user
    host = params.host 
    port = params.port 
    password = params.password
    db_name = params.db_name
    table_name = params.table_name
    file_url = params.file_url

    print(f'The source file is {file_url}')

    df_iter = pd.read_csv(file_url,low_memory=False, iterator=True, chunksize=100000, compression= 'gzip',
                          parse_dates= ['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    
    for chunk in df_iter:
        print('Inserting a chunk into the table')
        chunk.to_sql( name=table_name, con= engine, if_exists='append')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='Postgres Username')
    parser.add_argument('--password', required=True, help='Postgres Password')
    parser.add_argument('--host', required=True, help='Postgres Host')
    parser.add_argument('--port', required=True, help='Postgres Port')
    parser.add_argument('--db_name', required=True, help='Postgres Databasename')
    parser.add_argument('--table_name', required=True, help='Postgres Tablename')
    parser.add_argument('--file_url', required=True, help='Source File URL')
    
    args = parser.parse_args()

    ingest_data(args)