from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, year, month, dayofmonth, dayofweek, quarter, weekofyear, expr, monotonically_increasing_id
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

container_postgres_url = "jdbc:postgresql://postgres:5432/bix_challenger"
container_postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("DimensaoTempoComIDPortugues") \
    .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
    .config("spark.sql.session.local", "pt_BR") \
    .getOrCreate()


def generate_date_range(start_date, end_date):
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append((current_date,))
        current_date += timedelta(days=1)
    return date_list


start_date = datetime(2017, 1, 1)
end_date = datetime(2020, 12, 31)

date_data = generate_date_range(start_date, end_date)

df = spark.createDataFrame(date_data, ['data'])
windowSpec = Window.orderBy("data")
df_dim_tempo = df \
    .withColumn("id", row_number().over(windowSpec)) \
    .withColumn("ano", year(col("data"))) \
    .withColumn("mes", month(col("data"))) \
    .withColumn("dia", dayofmonth(col("data"))) \
    .withColumn("dia_da_semana", dayofweek(col("data"))) \
    .withColumn("trimestre", quarter(col("data"))) \
    .withColumn("semana_do_ano", weekofyear(col("data"))) \
    .withColumn("final_de_semana", expr("CASE WHEN dayofweek(data) IN (1, 7) THEN True ELSE False END"))

df_dim_tempo.write.jdbc(url=container_postgres_url, table="gold.dim_tempo", mode="append", properties=container_postgres_properties)
