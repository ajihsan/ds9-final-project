from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ajihsan',
    'start_date': datetime(2021, 1, 1),
    "retries": 0
}

dag = DAG('dag_batch_python', description = 'MongoDB to Postgres using Python', catchup = False, schedule_interval = None, default_args = default_args)

s1 = BashOperator(
    task_id = "transform_mongo",
    bash_command = "python3 /opt/airflow/scripts/transform_mongo_to_postgres.py",
    dag = dag
)
