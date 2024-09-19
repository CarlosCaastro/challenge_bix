from pyspark.sql import SparkSession

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
    .appName("Postgres to Postgres") \
    .getOrCreate()

df = spark.read.jdbc(url=external_postgres_url, table="public.venda", properties=external_postgres_properties)

df.write.jdbc(url=container_postgres_url, table="bronze.venda", mode="overwrite", properties=container_postgres_properties)

spark.stop()
