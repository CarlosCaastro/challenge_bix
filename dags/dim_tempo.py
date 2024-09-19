import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark-master:7077"

spark_app_name = "Dimensao de Tempo"
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
    dag_id="dim_tempo",
    description="This DAG triggers a Spark job to process data from GCS (Parquet file) and write to PostgreSQL",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

start = DummyOperator(task_id="start", dag=dag)

dim_tempo = SparkSubmitOperator(
    task_id="dim_tempo",
    application="jobs/python/d_tempo.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    packages="org.postgresql:postgresql:42.2.20",
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

start >> dim_tempo >> end