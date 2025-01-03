from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from modules.etl import *
from datetime import datetime
import os
from datawarehouse.main import *

# DWH Fact Dim
def fun_upsert_fact_dim():
    print("START fun_upsert_fact_dim  ")
    etl_process_postgresql_fact()
    etl_process_postgresql_dim('dim_payroll_query.sql', 'dim_payroll', ["EmployeeID", "PaymentDate"])
    etl_process_postgresql_dim('dim_training_query.sql', 'dim_training', ["EmployeeID", "StartDate"])
    etl_process_postgresql_dim('dim_performance_query.sql', 'dim_performance', ["EmployeeID", "ReviewPeriod"])
    etl_process_postgresql_dim('dim_candidate_query.sql', 'dim_candidate', ["CandidateID"])
    print("END fun_upsert_fact_dim")

# Data Mart
def fun_extract_transform():
    extract_load() 

def fun_load_etl():
    json_key_path = './dags/modules/digitalskola_key.json'
    load_etl(json_key_path) 

with DAG(
    dag_id='datamart',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    op_extract_transform = PythonOperator(
        task_id='extract_transform',
        python_callable = fun_extract_transform
    )

    op_load_etl = PythonOperator(
        task_id='load_etl',
        python_callable = fun_load_etl
    )

    op_upsert_fact_dim = PythonOperator(
        task_id='upsert_fact_dim',
        python_callable = fun_upsert_fact_dim
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task  >> op_upsert_fact_dim >> op_extract_transform >> op_load_etl >> end_task