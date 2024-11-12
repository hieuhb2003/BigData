import yaml
from pyspark.sql import SparkSession

class WarehouseLoader:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            self.pg_config = config['postgresql']
        
        self.jdbc_url = f"jdbc:postgresql://{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"

    def load_dataframe(self, df, table_name):
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.pg_config['user']) \
            .option("password", self.pg_config['password']) \
            .mode("overwrite") \
            .save()

    def load_rankings(self, rankings_df):
        self.load_dataframe(rankings_df, "daily_rankings")

    def load_movies(self, movies_df):
        self.load_dataframe(movies_df, "movies")
