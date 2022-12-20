from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'ajihsan',
    'start_date': datetime(2021, 1, 1),
    "retries": 0
}

dag = DAG('dag_batch_spark', description = 'CSV to Postgres using Spark', catchup = False, schedule_interval = None, default_args = default_args)

s1 = SparkSubmitOperator(
    task_id = "spark-csv",
    application = "/usr/local/spark/app/csv_to_mysql.py",
    name="csv to mysql",
    jars="/usr/local/spark/resources/mysql-connector-java-8.0.30.jar",
    conn_id = "spark_default",
    dag = dag
)

s2 = SparkSubmitOperator(
    task_id = "spark-mysql",
    application = "/usr/local/spark/app/mysql_to_postgres.py",
    name="mysql to postgresql",
    jars="/usr/local/spark/resources/mysql-connector-java-8.0.30.jar,/usr/local/spark/resources/postgresql-42.5.1.jar",
    conn_id = "spark_default",
    dag = dag
)

s1 >> s2
