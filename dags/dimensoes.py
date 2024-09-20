import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark-master:7077"

spark_app_name = "Dimensoes Gold"
now = datetime.now()

default_args = {
    'owner': 'Carlos',
    'start_date': datetime(now.year, now.month, now.day),
}

dag = DAG(
    'Dimensions',
    default_args=default_args,
    description='Submit Spark job to read from Postgres and write to gold schema',
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags = ["DIMENSÕES"]
)


spark_script_path = "jobs/python/bronze_to_gold.py"

jdbc_url = "jdbc:postgresql://postgres:5432/bix_challenger"
pg_user = "airflow"
pg_password = "airflow"
tabela_origem_categoria = "bronze.categoria"
schema_destino = "gold"
tabela_destino_d_categoria = "dim_categoria"

tabela_origem_employees = "bronze.employees"
tabela_destino_d_empregados = "dim_empregados"

spark_categoria = SparkSubmitOperator(
    task_id="d_categoria",
    application=spark_script_path,
    name="spark_postgres_to_gold_job",
    conn_id=spark_conn,
    verbose=1,
    application_args=[
        jdbc_url, pg_user, pg_password, tabela_origem_categoria, schema_destino, tabela_destino_d_categoria
    ],
    conf={
        "spark.master": spark_master
    },
    packages="org.postgresql:postgresql:42.2.20",
    dag=dag,
)

spark_employees = SparkSubmitOperator(
    task_id="d_empregado",
    application=spark_script_path,
    name="spark_postgres_to_gold_job",
    conn_id=spark_conn,
    verbose=1,
    application_args=[
        jdbc_url, pg_user, pg_password, tabela_origem_employees, schema_destino, tabela_destino_d_empregados
    ],
    conf={
        "spark.master": spark_master
    },
    packages="org.postgresql:postgresql:42.2.20",
    dag=dag,
)

spark_tempo = SparkSubmitOperator(
    task_id="d_tempo",
    application="jobs/python/d_tempo.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    packages="org.postgresql:postgresql:42.2.20",
    dag=dag
)

spark_categoria >> spark_employees >> spark_tempo
