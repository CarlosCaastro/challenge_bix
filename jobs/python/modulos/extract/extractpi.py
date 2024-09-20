import requests
from modulos.extract.extract import Extract
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class ExtracaoAPI(Extract):
    def __init__(self, spark_session: SparkSession, api_url: str):
        super().__init__(spark_session)
        self.api_url = api_url

    def fetch_employee_name(self, employee_id):
        try:
            response = requests.get(f"{self.api_url}?id={employee_id}")
            if response.status_code == 200:
                return employee_id, response.text
            else:
                return employee_id, f"Error {response.status_code}"
        except Exception as e:
            return employee_id, str(e)

    def extrair_dados(self, range_ids):
        data = [self.fetch_employee_name(employee_id) for employee_id in range_ids]
        schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True)
        ])
        return self.spark.createDataFrame(data, schema)
