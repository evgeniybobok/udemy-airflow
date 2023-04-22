from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def _my_func():
    print('Hello from my_func')

with DAG('python_dag',
    description='Python DAG',
    schedule_interval='@daily',
    start_date=datetime(2022, 11, 1),
    catchup=False,
    tags=['evgeniy']) as dag:
    dummy_task 	= DummyOperator(task_id='dummy_task', retries=3)
    python_task	= PythonOperator(task_id='python_task', python_callable=_my_func)

    dummy_task >> python_task
