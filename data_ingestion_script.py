import pandas as pd
from sqlalchemy import create_engine, text
import argparse


def ingest_data(params):
    """
    Pipeline to ingest the data into partitions in Postgres Database
    by dividing the data into small chunks
    """
    user = params.user
    host = params.host
    port = params.port
    password = params.password
    db_name = params.db_name
    table_name = params.table_name
    file_url = params.file_url

    print(f"The source file is {file_url}")

    # Reading the File
    df_iter = pd.read_csv(
        file_url,
        low_memory=False,
        iterator=True,
        chunksize=100000,
        compression="gzip",
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    )

    # Creating the Engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")

    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} "))

        # print(pd.io.sql.get_schema(df_iter, name='taxi_table', con= engine))

        main_table_with_partition_sql_statement = f"""
        CREATE TABLE {table_name} (
            "VendorID" BIGINT, 
            tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
            tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
            passenger_count BIGINT, 
            trip_distance FLOAT(53), 
            "RatecodeID" BIGINT, 
            store_and_fwd_flag TEXT, 
            "PULocationID" BIGINT, 
            "DOLocationID" BIGINT, 
            payment_type BIGINT, 
            fare_amount FLOAT(53), 
            extra FLOAT(53), 
            mta_tax FLOAT(53), 
            tip_amount FLOAT(53), 
            tolls_amount FLOAT(53), 
            improvement_surcharge FLOAT(53), 
            total_amount FLOAT(53), 
            congestion_surcharge FLOAT(53)
        ) PARTITION BY LIST("VendorID")
    """

        conn.execute(text(main_table_with_partition_sql_statement))
        print(f"The Main table is created with the schema {table_name}")

        # Creating Dynamic Partitions
        vendor_list = [1, 2]  # next(df_iter).VendorID.unique().tolist()
        for vendor in vendor_list:
            partition_sql_statement = f"""
                CREATE TABLE Vendor_ID_{vendor} PARTITION OF {table_name}
                FOR VALUES IN ({vendor})  
         """
            conn.execute(text(partition_sql_statement))
            print(
                f"The Partitioned table Vendor_ID_{vendor} is created with the schema"
            )
            # conn.commit()

        # Creating Default Partition
        conn.execute(
            text(f"CREATE TABLE Vendor_ID_others PARTITION OF {table_name} DEFAULT")
        )
        conn.commit()

    for index, chunk in enumerate(df_iter):
        # print(chunk.columns.tolist())
        print(f"**************Inserting chunk {index} into the table*****************")
        chunk.to_sql(name=table_name, con=engine, if_exists="append", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="Postgres Username")
    parser.add_argument("--password", required=True, help="Postgres Password")
    parser.add_argument("--host", required=True, help="Postgres Host")
    parser.add_argument("--port", required=True, help="Postgres Port")
    parser.add_argument("--db_name", required=True, help="Postgres Databasename")
    parser.add_argument("--table_name", required=True, help="Postgres Tablename")
    parser.add_argument("--file_url", required=True, help="Source File URL")

    args = parser.parse_args()

    ingest_data(args)


# To fix Autocommit error: https://github.com/sqlalchemy/sqlalchemy/issues/5405
# https://stackoverflow.com/questions/74706309/sqlalchemy-2-0-with-engine-connect-does-not-automatically-commit
