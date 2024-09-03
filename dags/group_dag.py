from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_downloads import subdag_downloads
from subdags.subdag_transform import subdag_transform

with DAG('group_dag', start_date=datetime(2021,1,1), catchup=False, schedule='@daily') as dag:

    subdag_args = {'start_date':dag.start_date, 'catchup':dag.catchup, 'schedule':dag.schedule_interval}

    downloads = SubDagOperator(task_id='downloads', subdag=subdag_downloads(dag.dag_id, 'downloads', subdag_args))

    # download_a = BashOperator(task_id="download_a", bash_command='sleep 10')
    # download_b = BashOperator(task_id="download_b", bash_command='sleep 10')
    # download_c = BashOperator(task_id="download_c", bash_command='sleep 10')

    check_files = BashOperator(task_id="check_files", bash_command="sleep 10")
    

    transforms = SubDagOperator(task_id="transforms", subdag=subdag_transform(dag.dag_id, "transforms", subdag_args))
    # transform_a = BashOperator(task_id="transform_a", bash_command="sleep 10")
    # transform_b = BashOperator(task_id="transform_b", bash_command="sleep 10")
    # transform_c = BashOperator(task_id="transform_c", bash_command="sleep 10")

    # [download_a, download_b, download_c] >> check_files >> [transform_a, transform_b, transform_c]
    downloads >> check_files >> transforms

