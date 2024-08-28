from datetime import datetime
from airflow import DAG, Dataset
from airflow.decorators import task

my_file = Dataset('/tmp/my_file.txt')

with DAG('producer', start_date=datetime(2022,1,1), schedule="@daily", catchup=False):

    @task(outlets=[my_file])
    def update_dateset():
        with open(my_file.uri,'a+') as f:
            f.write(f"\nLine added at time {datetime.now()}")
    
    update_dateset()