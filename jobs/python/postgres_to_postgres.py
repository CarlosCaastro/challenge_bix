from pyspark.sql import SparkSession
from modulos.extract.extractpostgres import ExtracaoPostgres
from modulos.load.load_postgresql import LoadPostgresql

container_postgres_url = "jdbc:postgresql://postgres:5432/bix_challenger"
container_postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

external_postgres_url = "jdbc:postgresql://34.173.103.16:5432/postgres"
external_postgres_properties = {
    "user": "junior",
    "password": "|?7LXmg+FWL&,2(",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Extract from Postgres to load in another Postgres") \
    .getOrCreate()

schema_extract = 'public'
table_name_extract = 'venda'

table_schema_extract = f'{schema_extract}.{table_name_extract}'

schema_load = 'bronze'
table_name_load = 'venda'

extract_postgres_bix = ExtracaoPostgres(spark,external_postgres_url, external_postgres_properties, table_schema_extract)
load_postgres_local = LoadPostgresql(spark, container_postgres_url, container_postgres_properties)

df_bix = extract_postgres_bix.extrair_dados()
load_postgres_local.carregar_no_postgres(df_bix, f'{schema_load}.{table_name_load}', "overwrite")

spark.stop()
