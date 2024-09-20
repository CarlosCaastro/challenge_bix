from pyspark.sql import SparkSession
from modulos.extract.extractfile import ExtracaoArquivo
from modulos.load.load_postgresql import LoadPostgresql

container_postgres_url = "jdbc:postgresql://postgres:5432/bix_challenger"
container_postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

parquet_path= "/opt/data/categoria.parquet"
schema = 'bronze'
table_name = 'categoria'

spark = SparkSession.builder.appName("Parquet to Postgres").getOrCreate()

extracao_arquivo = ExtracaoArquivo(spark_session=spark, arquivo_path=parquet_path, formato="parquet")
load_postgres_local = LoadPostgresql(spark, container_postgres_url, container_postgres_properties)

df_categoria = extracao_arquivo.extrair_dados()
load_postgres_local.carregar_no_postgres(df_categoria, f'{schema}.{table_name}', "overwrite")


spark.stop()


