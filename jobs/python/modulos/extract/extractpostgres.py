from pyspark.sql import SparkSession
from modulos.extract.extract import Extract

class ExtracaoPostgres(Extract):
    def __init__(self, spark_session: SparkSession, jdbc_url: str, properties: dict, tabela: str):
        super().__init__(spark_session)
        self.jdbc_url = jdbc_url
        self.properties = properties
        self.tabela = tabela

    def extrair_dados(self):
        return self.spark.read.jdbc(url=self.jdbc_url, table=self.tabela, properties=self.properties)
