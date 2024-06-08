from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file_1 = Dataset("/tmp/my_file_1.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")

with DAG(
    dag_id="consumer",
    schedule=[my_file_1, my_file_2],
    start_date=datetime(2022,1,1),
    catchup=False
):

    @task
    def read_dataset():
        with open(my_file_1.uri, "r") as f:
            print(f.read())
    
    read_dataset()
