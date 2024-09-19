import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark-master:7077"

spark_app_name = "Postgres to Postgres"
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="postgres_to_postgres",
    description="This DAG reads data from an external PostgreSQL and writes it to a containerized PostgreSQL",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

spark_job_postgres_to_postgres = SparkSubmitOperator(
    task_id="spark_job_postgres_to_postgres",
    application="jobs/python/postgres_to_postgres.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    packages="org.postgresql:postgresql:42.2.20", 
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_postgres_to_postgres >> end
