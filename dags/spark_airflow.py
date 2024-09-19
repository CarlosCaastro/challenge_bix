import airflow
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark-master:7077"
spark_app_name = "Spark Count Words"

dag = DAG(
    dag_id = "sparking_flow",
    default_args = {
        "owner": "Yusuf Ganiyu",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="spark_job",
    application="jobs/python/wordcountjob.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    # application_args=[file_path],
    dag=dag)

# python_job = SparkSubmitOperator(
#     task_id="python_job",
#     conn_id=spark_conn,
#     application="jobs/python/wordcountjob.py",
#     dag=dag
# )


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> end
