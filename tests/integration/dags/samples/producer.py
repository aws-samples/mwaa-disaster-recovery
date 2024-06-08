from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file_1 = Dataset("/tmp/my_file_1.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")

with DAG(
    dag_id="producer",
    schedule=None,
    start_date=datetime(2022,1,1),
    catchup=False
):

    @task(outlets=[my_file_1])
    def update_dataset_1():
        with open(my_file_1.uri, "a+") as f:
            f.write("producer update")
    
    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, "a+") as f:
            f.write("producer update")
    
    update_dataset_1()
    update_dataset_2()
