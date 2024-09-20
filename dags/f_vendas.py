import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark-master:7077"

spark_app_name = "F Vendas"
now = datetime.now()

default_args = {
    "owner": "Carlos",
    "start_date": datetime(now.year, now.month, now.day),
}

dag = DAG(
    dag_id="f_vendas",
    description="This DAG triggers a Spark job to process data from GCS (Parquet file) and write to PostgreSQL",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags = ["FATO"]
)

start = DummyOperator(task_id="start", dag=dag)

f_vendas = SparkSubmitOperator(
    task_id="f_vendas",
    application="jobs/python/f_vendas.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    packages="org.postgresql:postgresql:42.2.20",
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

start >> f_vendas >> end