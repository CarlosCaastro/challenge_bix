from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from modulos.extract.extractpostgres import ExtracaoPostgres
from modulos.load.load_postgresql import LoadPostgresql

container_postgres_url = "jdbc:postgresql://postgres:5432/bix_challenger"
container_postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("SubstituirDataVendaPorIDTempo") \
    .getOrCreate()

extract_postgres_vendas = ExtracaoPostgres(spark,container_postgres_url, container_postgres_properties, "bronze.venda")
extract_postgres_tempo = ExtracaoPostgres(spark,container_postgres_url, container_postgres_properties, "gold.dim_tempo")
load_postgres_local = LoadPostgresql(spark, container_postgres_url, container_postgres_properties)

df_vendas = extract_postgres_vendas.extrair_dados()

df_dim_tempo = extract_postgres_tempo.extrair_dados()

df_dim_tempo = df_dim_tempo.withColumn("data_date", to_date(col("data")))

df_vendas_com_tempo = df_vendas.join(
    df_dim_tempo,
    df_vendas["data_venda"] == df_dim_tempo["data_date"],
    "inner"
)

df_vendas_com_tempo = df_vendas_com_tempo \
    .withColumnRenamed("id", "id_tempo") \
    .drop("data_venda") \
    .drop("data_date")

df_vendas_reorganizado = df_vendas_com_tempo.select(
    col("id_venda"),
    col("id_funcionario"),
    col("id_categoria"),
    col("id_tempo"),
    col("venda")
)

load_postgres_local.carregar_no_postgres(df_vendas_reorganizado, "gold.f_vendas", "overwrite")

spark.stop()
