from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("PostgresToGoldSchema") \
    .getOrCreate()

jdbc_url = sys.argv[1]
pg_user = sys.argv[2]
pg_password = sys.argv[3]
tabela_origem = sys.argv[4]
schema_destino = sys.argv[5]
tabela_destino = sys.argv[6]

properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table=tabela_origem, properties=properties)

df.write.mode("append").jdbc(url=jdbc_url, table=f"{schema_destino}.{tabela_destino}", properties=properties)

spark.stop()
