
class LoadPostgresql:
    def __init__(self, spark_session, jdbc_url, properties):
        self.spark = spark_session
        self.jdbc_url = jdbc_url
        self.properties = properties

    def carregar_no_postgres(self, df, tabela, modo="append"):
        df.write.jdbc(url=self.jdbc_url, table=tabela, mode=modo, properties=self.properties)

