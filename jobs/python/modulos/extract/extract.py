from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class Extract(ABC):
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    @abstractmethod
    def extrair_dados(self):
        pass
