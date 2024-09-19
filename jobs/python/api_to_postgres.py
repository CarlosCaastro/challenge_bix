from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests

container_postgres_url = "jdbc:postgresql://postgres:5432/bix_challenger"
container_postgres_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

def fetch_employee_name(employee_id):
    url = f"https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={employee_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.text
            return employee_id, data
        else:
            return employee_id, f"Error {response.status_code}"
    except Exception as e:
        return employee_id, str(e)

spark = SparkSession.builder \
    .appName("Escrita no banco Postgres") \
    .getOrCreate()

data = []
for employee_id in range(1, 10):
    result = fetch_employee_name(employee_id)
    data.append(result)

schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True)
])

employee_df = spark.createDataFrame(data, schema)


employee_df.write.jdbc(url=container_postgres_url, table="bronze.employees", mode="overwrite", properties=container_postgres_properties)


spark.stop()
