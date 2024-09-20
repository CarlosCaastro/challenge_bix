from pyspark.sql import SparkSession
import sys
from modulos.extract.extractpostgres import ExtracaoPostgres
from modulos.load.load_postgresql import LoadPostgresql

spark = SparkSession.builder \
    .appName("Dimensoes Case") \
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

extract_postgres_local = ExtracaoPostgres(spark,jdbc_url, properties, tabela_origem)
load_postgres_local = LoadPostgresql(spark, jdbc_url, properties)

df = extract_postgres_local.extrair_dados()
load_postgres_local.carregar_no_postgres(df, f'{schema_destino}.{tabela_destino}', "overwrite")

# df = spark.read.jdbc(url=jdbc_url, table=tabela_origem, properties=properties)

# df.write.mode("append").jdbc(url=jdbc_url, table=f"{schema_destino}.{tabela_destino}", properties=properties)

spark.stop()
