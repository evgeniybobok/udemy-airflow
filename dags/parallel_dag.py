from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
 
with DAG('parallel_dag',
         start_date=datetime(2023, 4, 1),
         schedule_interval='@daily',
         catchup=False,
         tags=['evgeniy']) as dag:
 
    extract_a = BashOperator(
        task_id='extract_a',
        bash_command='sleep 5'
    )
 
    extract_b = BashOperator(
        task_id='extract_b',
        bash_command='sleep 5'
    )
 
    load_a = BashOperator(
        task_id='load_a',
        bash_command='sleep 5'
    )
 
    load_b = BashOperator(
        task_id='load_b',
        bash_command='sleep 5'
    )
 
    transform = BashOperator(
        task_id='transform',
        queue='ml_model',
        bash_command='sleep 30'
    )
 
    extract_a >> load_a
    extract_b >> load_b
    [load_a, load_b] >> transform
