from modulos.extract.extract import Extract
from pyspark.sql import SparkSession

class ExtracaoArquivo(Extract):
    def __init__(self, spark_session: SparkSession, arquivo_path: str, formato: str = "parquet"):
        super().__init__(spark_session)
        self.arquivo_path = arquivo_path
        self.formato = formato

    def extrair_dados(self):
        leitor = self.spark.read.format(self.formato)
        
        if self.formato == "csv":
            leitor = leitor.option("header", True).option("inferSchema", True)
        
        return leitor.load(self.arquivo_path)
