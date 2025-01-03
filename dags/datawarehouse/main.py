import os
# import connection
from datawarehouse.connection import config, get_conn, get_conn_mysql
import sqlparse
import pandas as pd
import time
from pymongo import MongoClient
import numpy as np
import json
import psycopg2 as pg
from datawarehouse.model import modelRecruitment

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

def upsert_into_table(conn, df, table, idx):
    # Convert DataFrame rows into tuples
    tuples = [tuple(x) for x in df.to_numpy()]
    
    # Generate column names for INSERT statement, excluding `PayrollID`
    cols = ', '.join([f'"{col}"' for col in df.columns])  # Ensure column names are quoted
    
    # Create a string for the update part of the query, excluding index columns
    updated_cols = ', '.join([f'"{col}"=EXCLUDED."{col}"' for col in df.columns if col not in idx])
    
    # Create placeholders for the values to be inserted
    placeholders = ', '.join(["%s"] * len(df.columns))
    
    # Generate the conflict columns (index columns)
    idx_cols = ', '.join([f'"{col}"' for col in idx])  # Ensure index column names are quoted

    # Construct the upsert query
    if updated_cols:  # If there are columns to update
        query = f"""
            INSERT INTO {table} ({cols}) VALUES ({placeholders})
            ON CONFLICT ({idx_cols}) DO UPDATE SET {updated_cols}
        """
    else:  # If no columns to update, fallback to `DO NOTHING`
        query = f"""
            INSERT INTO {table} ({cols}) VALUES ({placeholders})
            ON CONFLICT ({idx_cols}) DO NOTHING
        """
    
    # print("Query:", query)  # Debug log
    # print("Tuples:", tuples[:5])  # Debug log (show first 5 rows)

    try:
        with conn.cursor() as cursor:
            # Use executemany to perform the upsert
            cursor.executemany(query, tuples)
            conn.commit()
        print(f"[UPSERT DONE] {table}")
    except (Exception, pg.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
        return 1

    return 0

def insert_into_table(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join([f'"{col}"' for col in df.columns])  # Ensure column names are quoted
    
    placeholders = ', '.join(["%s"] * len(df.columns))
    query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, tuples)
            conn.commit()
        print(f"[INSERT DONE] {table}")
        time.sleep(10)
    except (Exception, pg.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
        return 1

    return 0

def get_connection(name_source, db_type='postgresql'):
    conf = config(name_source)
    if not conf:
        raise ValueError(f"Invalid configuration for {name_source}")
    
    if db_type == 'mysql':
        conn, engine = get_conn_mysql(conf, name_source)
    else:  # default to postgresql
        result = get_conn(conf, name_source)
        if result is None:
            raise ConnectionError(f"Failed to connect to {name_source}")
        conn, engine = result
    
    cursor = conn.cursor()
    return conn, engine, cursor

def read_sql_query(file_name):
    # Get the Airflow home directory, defaulting to /opt/airflow if not set
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')  # Default to /opt/airflow
    file_path = os.path.join(AIRFLOW_HOME, 'query', file_name)

    # Debugging output to verify file path and available files
    print(f"Looking for SQL file at: {file_path}")
    try:
        # List files inside the 'query' directory for debugging
        available_files = os.listdir(os.path.join(AIRFLOW_HOME, 'query'))
        print(f"Available files in {os.path.join(AIRFLOW_HOME, 'query')}: {available_files}")
    except Exception as e:
        print(f"Error reading the directory: {e}")
    
    # Check if the SQL file exists at the constructed path
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file '{file_name}' not found at {file_path}")
    
    # Read the SQL file
    with open(file_path, 'r') as file:
        sql_query = file.read()
    
    return sql_query

# Function to ingest data to DWH
def ingest_data_to_dwh(df, table_name, engine_dwh):
    df.to_sql(
        table_name,
        engine_dwh,
        schema='public',
        if_exists='replace',
        index=False
    )
    print(f'[INFO] {table_name} data ingested successfully...')

# Main ETL function for PostgreSQL
def etl_process_postgresql():
    # connection data source (PostgreSQL)
    conn, engine, cursor = get_connection('marketplace_prod')

    # connection dwh
    conn_dwh, engine_dwh, cursor_dwh = get_connection('dwh')

    # get query strings
    data_payroll_query = read_sql_query('data_payroll_query.sql')
    data_performance_query = read_sql_query('data_performance_query.sql')
    data_training_query = read_sql_query('data_training_query.sql')

    try:
        print('[INFO] service etl is running...')

        # Data Payroll ETL
        print('[INFO] Fetching data for payroll...')
        df_payroll = pd.read_sql(data_payroll_query, engine)
        print(df_payroll.head(5))
        ingest_data_to_dwh(df_payroll, 'data_payroll_champy_raw', engine_dwh)
        
        time.sleep(10)

        # Data Performance ETL
        print('[INFO] Fetching data for performance...')
        df_performance = pd.read_sql(data_performance_query, engine)
        print(df_performance.head(5))
        ingest_data_to_dwh(df_performance, 'data_performance_champy_raw', engine_dwh)
        
        time.sleep(10)

    except Exception as e:
        print('[ERROR] service etl is failed')
        print(str(e))

# Main ETL function for MySQL
def etl_process_mysql():
    # connection data source (MySQL)
    conn, engine, cursor = get_connection('marketplace_prod_mysql', db_type='mysql')

    # connection dwh
    conn_dwh, engine_dwh, cursor_dwh = get_connection('dwh')

    # get query strings
    data_training_query = read_sql_query('data_training_query.sql')

    try:
        print('[INFO] service etl is running...')

        # Data Training ETL
        print('[INFO] Fetching data for training...')
        df_training = pd.read_sql(data_training_query, engine)
        print(df_training.head(5))

        # Ingest Training Data to DWH
        ingest_data_to_dwh(df_training, 'data_training_champy_raw', engine_dwh)

        time.sleep(10)

    except Exception as e:
        print('[ERROR] service etl is failed')
        print(str(e))

# Main ETL dim_fact function for PostgreSQL
def etl_process_postgresql_fact():
    # connection dwh
    conn_dwh, engine_dwh, cursor_dwh = get_connection('dwh')

    # get query strings
    fact_employee_query = read_sql_query('fact_employee_query.sql')

    try:
        print('[INFO] service etl fact_dim is running...')

        # Data fact_employee_query ETL
        print('[INFO] Fetching data for fact_employee...')
        df_fact_employee = pd.read_sql(fact_employee_query, engine_dwh)
        print(df_fact_employee.head(5))
        # ingest_data_to_dwh(df_fact_employee, 'fact_employee', engine_dwh)
        # error_status = insert_into_table(conn_dwh, df_fact_employee, 'fact_employee')
        error_upsert_status = upsert_into_table(conn_dwh, df_fact_employee, 'fact_employee',['EmployeeID'])
        # If upsert fails, insert the first row as a fallback
        if error_upsert_status == 1:
            print("[INFO] Upsert failed. Inserting the first row as fallback...")
            error_insert_status = insert_into_table(conn_dwh, df_fact_employee[:1], 'fact_employee')

        time.sleep(10)

    except Exception as e:
        print('[ERROR] service etl fact_dim is failed')
        print(str(e))

def etl_process_postgresql_dim(query_file, table, idx):

    try:
        # Establish connection
        conn_dwh, engine_dwh, cursor_dwh = get_connection('dwh')

        # Read the SQL query from the file
        dim_query = read_sql_query(query_file)
        print(f"[INFO] Service ETL for {query_file} is running...")

        # Fetch data for the specified query
        print(f"[INFO] Fetching data for {query_file}...")
        df = pd.read_sql(dim_query, engine_dwh)
        print(f"[INFO] Fetched {len(df)} rows for {query_file}.")
        print(df.head(5))  # Preview the first 5 rows of the data

        # Insert data into the target table
        # error_status = insert_into_table(conn_dwh, df, table)
        error_upsert_status = upsert_into_table(conn_dwh, df, table, idx)

        if error_upsert_status == 1:
            error_insert_status = insert_into_table(conn_dwh, df[:1], table)
            print(f"[ERROR] Failed to insert data into {table}.")

    except Exception as e:
        print(f"[ERROR] ETL process for {query_file} failed: {e}")

    finally:
        # Close the connection
        if conn_dwh:
            conn_dwh.close()
            print("[INFO] Database connection closed.")

# for mongo config
def load_config(config_file='config.json'):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

# Helper function to connect to MongoDB and fetch data
def fetch_data_from_mongodb(mongo_uri, database_name, collection_name):
    try:
        # Connect to MongoDB server
        server = MongoClient(mongo_uri)
        
        # Verify the connection
        db = server.admin
        server_status = db.command("ping")
        print("MongoDB connection successful:", server_status)

        # Access the specified database and collection
        db = server[database_name]
        collection = db[collection_name]
        print(f"Using database: {database_name}")
        print(f"Using collection: {collection_name}")

        # Query the collection and convert to a DataFrame
        documents = collection.find()
        data_list = list(documents)  # Convert cursor to a list
        df = pd.DataFrame(data_list)  # Create a DataFrame from the list

        # Replace NaN, inf, and -inf with None
        df = df.replace([np.nan, np.inf, -np.inf], None)

        # Drop unwanted columns
        df.drop(columns=['_id'], inplace=True, axis=1)

        print("Data fetched and cleaned successfully.")

        return df

    except Exception as e:
        print("An error occurred:", e)
        return None

# Function to ingest MongoDB data into DWH
def ingest_mongodb_data_to_dwh(mongo_uri, database_name, collection_name, table_name):
    conn_dwh, engine_dwh, cursor_dwh = get_connection('dwh')
    df = fetch_data_from_mongodb(mongo_uri, database_name, collection_name)
    print("mongo df: ")
    print(df.head(5))
    
    df['Prediction'] = df.apply(process_row, axis=1)

    print("mongo df AFTER MODELS: ")
    print(df.head(5))
    
    if df is not None:
        try:
            # Ingest the DataFrame to DWH
            df.to_sql(
                table_name,
                engine_dwh,
                schema='public',
                if_exists='replace',
                index=False
            )
            print(f'[INFO] {table_name} data ingested successfully...')
        except Exception as e:
            print('[ERROR] Failed to ingest data into DWH')
            print(str(e))
    else:
        print('[ERROR] Failed to fetch data from MongoDB.')

def process_row(row):
    # Create a dictionary with required keys
    data_dict = {key: row[key] for key in ['Gender', 'Age', 'Position', 'Status'] if key in row}
    path = os.path.join(os.getcwd(), "model")
    result = modelRecruitment.runModel(data_dict, path)
    return result

def mongo_model():
    # conn_dwh, engine_dwh, cursor_dwh = get_connection('dwh')
    
    # Load MongoDB credentials
    config = load_config()
    mongo_uri = config['mongodb']['mongo_uri']
    database_name = config['mongodb']['database_name']
    collection_name = config['mongodb']['collection_name']    
    # Ingest MongoDB data into DWH
    ingest_mongodb_data_to_dwh(mongo_uri, database_name, collection_name, 'data_recruitment_champy_raw')

# Function to execute a given SQL file for DWH design
def dwh_design(script):
    conn_dwh = None
    cursor_dwh = None
    try:
        # Create DWH connection engine
        conn_dwh, engine_dwh, cursor_dwh = get_connection('dwh')

        # Read SQL script
        dwh_design_query = read_sql_query(script)

        # Execute SQL commands
        cursor_dwh.execute(dwh_design_query)
        conn_dwh.commit()

        print(f"DWH design and table creation using {script} completed successfully.")
    except Exception as e:
        print(f"Error in DWH design: {e}")
    finally:
        # Ensure that cursor and connection are closed only if they were initialized
        if cursor_dwh:
            cursor_dwh.close()
        if conn_dwh:
            conn_dwh.close()


if __name__ == "__main__":

    # Example usage
    dwh_design('dwh_design.sql')

    # Run ETL for PostgreSQL
    etl_process_postgresql()

    # Run ETL for MySQL
    etl_process_mysql()

    print("Load mongo_model")
    mongo_model()

    # run etl fact_dim
    etl_process_postgresql_fact()

    # conn_dwh, engine_dwh, cursor_dwh = get_connection('dwh')
    dwh_design('dwh_design_dim.sql')


    etl_process_postgresql_dim('dim_payroll_query.sql', 'dim_payroll', ["EmployeeID", "PaymentDate"])
    etl_process_postgresql_dim('dim_training_query.sql', 'dim_training', ["EmployeeID", "StartDate"])
    etl_process_postgresql_dim('dim_performance_query.sql', 'dim_performance', ["EmployeeID", "ReviewPeriod"])
    etl_process_postgresql_dim('dim_candidate_query.sql', 'dim_candidate', ["CandidateID"])

