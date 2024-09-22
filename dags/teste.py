from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

spark_master = "spark://spark-master:7077"
# Definindo os parâmetros da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_minio_job',
    default_args=default_args,
    description='Spark job para gravar dados no MinIO',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 9, 21),
    catchup=False,
)

submit_spark_job = SparkSubmitOperator(
    task_id='submit_spark_minio_job',
    application='jobs/python/gravar_no_minio.py',
    conn_id='spark_conn',
    application_args=[],
    packages='org.apache.hadoop:hadoop-aws:3.3.4',
    conf={
        'spark.hadoop.fs.s3a.access.key': 'minio',
        'spark.hadoop.fs.s3a.secret.key': 'minio123',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    dag=dag,
)

submit_spark_job
