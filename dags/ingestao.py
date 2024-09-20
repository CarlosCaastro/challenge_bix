import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark-master:7077"

spark_app_name = "Ingestion"
now = datetime.now()

default_args = {
    "owner": "Carlos",
    "start_date": datetime(now.year, now.month, now.day),
}

dag = DAG(
    dag_id="Ingestion",
    description="This DAG triggers a Spark job to process data from diferents locations and write to PostgreSQL",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags = ["INGESTION"]
)

start = DummyOperator(task_id="start", dag=dag)

python_download = BashOperator(
        task_id='executar_processamento',
        bash_command='python3 /opt/airflow/jobs/python/download_parquet.py',
    )

spark_parquet = SparkSubmitOperator(
    task_id="parquet_to_postgres",
    application="jobs/python/parquet_to_postgres.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    packages="org.postgresql:postgresql:42.2.20",
    dag=dag
)

spark_postgres = SparkSubmitOperator(
    task_id="spark_job_postgres_to_postgres",
    application="jobs/python/postgres_to_postgres.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    packages="org.postgresql:postgresql:42.2.20", 
    dag=dag
)

spark_api = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="jobs/python/api_to_postgres.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    packages="org.postgresql:postgresql:42.2.20",
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

start >> python_download >> spark_parquet >> spark_postgres >> spark_api >> end