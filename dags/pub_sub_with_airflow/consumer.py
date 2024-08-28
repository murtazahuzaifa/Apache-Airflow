from airflow import DAG, Dataset
from datetime import datetime
from airflow.decorators import task

my_file = Dataset('/tmp/my_file.txt')

with DAG('consumer', start_date=datetime(2021,1,1), schedule=[my_file], catchup=False):

    @task
    def read_dataset():
        with open(my_file.uri, 'r') as f:
            print(f.read())

    read_dataset()