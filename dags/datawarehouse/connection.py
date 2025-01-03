import os
import json
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pymysql
from psycopg2 import connect
from psycopg2.errors import OperationalError
from sqlalchemy.exc import SQLAlchemyError


def config(connection_db):
    path = os.path.join(os.getcwd(), 'dags', 'datawarehouse')  # Adjust path to correct location
    with open(os.path.join(path, 'config.json')) as file:
        conf = json.load(file)[connection_db]
        return conf

print(config('marketplace_prod'))



# Function to establish a database connection
def get_conn(conf, name_conn):
    try:
        # Establish psycopg2 connection
        conn = connect(
            host=conf['host'],
            database=conf['db'],
            user=conf['user'],
            password=conf['password'],
            port=conf['port']
        )
        print(f'[INFO] Successfully connected to PostgreSQL: {name_conn}')

        # Create SQLAlchemy engine
        engine = create_engine(
            "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
                conf['user'],
                quote_plus(conf['password']),  # Escape special characters
                conf['host'],
                conf['port'],
                conf['db']
            )
        )
        return conn, engine

    except OperationalError as psycopg_error:
        print(f'[ERROR] psycopg2 failed to connect to PostgreSQL: {name_conn}')
        print(str(psycopg_error))
        return None, None

    except SQLAlchemyError as sqlalchemy_error:
        print(f'[ERROR] SQLAlchemy failed to create an engine for: {name_conn}')
        print(str(sqlalchemy_error))
        return None, None

    except Exception as e:
        print(f'[ERROR] Unexpected error while connecting to PostgreSQL: {name_conn}')
        print(str(e))
        return None, None

print(config('marketplace_prod'))
def get_conn_mysql(conf, name_conn):
    try:
        # Establish connection using pymysql
        conn = pymysql.connect(
            host=conf['host'],
            database=conf['db'],
            user=conf['user'],
            password=conf['password'],
            port=int(conf['port'])
        )
        print(f'[INFO] Successfully connected to MySQL {name_conn}')
        
        # Create SQLAlchemy engine
        engine = create_engine(
            "mysql+pymysql://{}:{}@{}:{}/{}".format(
                conf['user'],
                quote_plus(conf['password']),  # Escapes special characters in password
                conf['host'],
                conf['port'],
                conf['db']
            )
        )
        return conn, engine

    except Exception as e:
        print(f'[ERROR] Failed to connect to MySQL {name_conn}')
        print(f'[ERROR] {e.__class__.__name__}: {str(e)}')
