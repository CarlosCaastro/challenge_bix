from pyspark.sql import SparkSession

postgres_url = "jdbc:postgresql://postgres:5432/bix_challenger"
postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

gcs_parquet_url = "/opt/data/categoria.parquet"

spark = SparkSession.builder.appName("GCS to Postgres").getOrCreate()

df = spark.read.parquet(gcs_parquet_url)

df.write.mode("overwrite").jdbc(url=postgres_url, table="bronze.categoria", properties=postgres_properties)

spark.stop()


