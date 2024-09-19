from pyspark.sql import SparkSession

# Cria a sessão do Spark
spark = SparkSession.builder.appName("SimpleDataFrame").getOrCreate()

# Exemplo de dados
data = [("Hello", 1), ("Spark", 2), ("Python", 3), ("Airflow", 4), ("Docker", 5), ("Yusuf", 6)]

# Cria um DataFrame a partir da lista de tuplas
df = spark.createDataFrame(data, ["word", "count"])

# Mostra o DataFrame
df.show()

# Finaliza a sessão do Spark
spark.stop()
