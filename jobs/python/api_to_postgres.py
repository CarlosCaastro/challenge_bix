from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests

from modulos.extract.extractpi import ExtracaoAPI
from modulos.load.load_postgresql import LoadPostgresql


spark = SparkSession.builder \
    .appName("Extract from API") \
    .getOrCreate()

container_postgres_url = "jdbc:postgresql://postgres:5432/bix_challenger"
container_postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

schema = 'bronze'
table_name = 'employees'

api_url = "https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior"
extract_api = ExtracaoAPI(spark, api_url)
load_postgres_local = LoadPostgresql(spark, container_postgres_url, container_postgres_properties)
df_api = extract_api.extrair_dados(range(1, 10))

load_postgres_local.carregar_no_postgres(df_api, f'{schema}.{table_name}', "overwrite")

spark.stop()