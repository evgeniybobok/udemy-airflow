from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task

my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")


with DAG(
    dag_id="producer_dag",
    schedule="@daily",
    start_date=datetime(2023, 4, 20),
    catchup=False,
    tags=['evgeniy']
):
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "w") as f:
            f.write("producer update\n")

    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, "w") as f:
            f.write("producer update 2\n")

    update_dataset()
    update_dataset_2()