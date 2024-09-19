from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

container_postgres_url = "jdbc:postgresql://postgres:5432/bix_challenger"
container_postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("SubstituirDataVendaPorIDTempo") \
    .getOrCreate()

df_vendas = spark.read.jdbc(url=container_postgres_url, table="bronze.venda", properties=container_postgres_properties)

df_dim_tempo = spark.read.jdbc(url=container_postgres_url, table="gold.dim_tempo", properties=container_postgres_properties)

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

df_vendas_reorganizado.write.jdbc(url=container_postgres_url, table="gold.f_vendas", mode="append", properties=container_postgres_properties)


spark.stop()
