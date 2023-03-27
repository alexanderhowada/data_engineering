import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../scripts")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from write_date import main

FOLDER = os.path.dirname(os.path.abspath(__file__)).split('/')[-2]
BASE_PATH = f'/opt/airflow/datasets/{FOLDER}/datasets'

dag = DAG(
    'python_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once'
)

print_date = PythonOperator(
    task_id='print_date',
    python_callable=main,
    op_kwargs={'file': f'{BASE_PATH}/date1.csv'},
    dag=dag
)

print_date_again = PythonOperator(
    task_id='print_date_again',
    python_callable=main,
    op_kwargs={'file': f'{BASE_PATH}/date2.csv'},
    dag=dag
)

print_date >> print_date_again

