from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder \
    .appName("MinIO Integration") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


data = [Row(name="Carlos", age=30), Row(name="Maria", age=25)]
df = spark.createDataFrame(data)

df.write \
    .mode("overwrite") \
    .parquet("s3a://raw/dados_exemplo/")

spark.stop()

