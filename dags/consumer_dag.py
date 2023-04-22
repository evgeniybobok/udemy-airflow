from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task

my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")

with DAG(
    dag_id="consumer_dag",
    schedule=[my_file, my_file_2],
    start_date=datetime(2023, 4, 20),
    catchup=False,
    tags=['evgeniy']
):
    @task()
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())
            
    @task()
    def read_dataset_2():
        with open(my_file_2.uri, "r") as f:
            print(f.read())

    read_dataset()
    read_dataset_2()