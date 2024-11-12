from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, regexp_replace, split, concat_ws
from pyspark.sql.types import *

class DataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("MovieAnalytics") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
            .getOrCreate()

    def process_rankings(self, rankings_data):
        # Define schema for rankings data
        movies_schema = StructType([
            StructField("Rank", StringType(), True),
            StructField("Rank Yesterday", StringType(), True),
            StructField("Release", StringType(), True),
            StructField("Daily Gross", StringType(), True),
            StructField("Gross Change/Day", StringType(), True),
            StructField("%± LW", StringType(), True),
            StructField("Theaters", StringType(), True),
            StructField("Average", StringType(), True),
            StructField("Gross to Date", StringType(), True),
            StructField("Days", StringType(), True),
            StructField("Distributor", StringType(), True),
            StructField("New", StringType(), True)
        ])

        rankings_schema = StructType([
            StructField("Date", StringType(), True),
            StructField("Day", StringType(), True),
            StructField("Month", StringType(), True),
            StructField("Year", StringType(), True),
            StructField("Movies", ArrayType(movies_schema), True)
        ])

        # Create DataFrame
        rankings_df = self.spark.createDataFrame([rankings_data], rankings_schema)
        
        # Explode Movies array and clean monetary values
        flattened_df = rankings_df.select(
            col("Date"),
            explode("Movies").alias("movie_data")
        ).select(
            "Date",
            col("movie_data.*")
        )

        # Clean monetary values
        cleaned_df = flattened_df.withColumn(
            "Daily_Gross_Clean",
            regexp_replace(regexp_replace(col("Daily Gross"), "\\$", ""), ",", "").cast("double")
        ).withColumn(
            "Gross_to_Date_Clean",
            regexp_replace(regexp_replace(col("Gross to Date"), "\\$", ""), ",", "").cast("double")
        )

        return cleaned_df

    def process_movies(self, movies_data):
        # Define schema for movies data
        movies_schema = StructType([
            StructField("Title", StringType(), True),
            StructField("Earliest Release Country", StringType(), True),
            StructField("Domestic Distributor", StringType(), True),
            StructField("Domestic Opening", StringType(), True),
            StructField("Earliest Release Date", StringType(), True),
            StructField("MPAA", StringType(), True),
            StructField("Running Time", StringType(), True),
            StructField("Genres", StringType(), True),
            StructField("Director", StringType(), True),
            StructField("Writer", ArrayType(StringType()), True),
            StructField("Producer", ArrayType(StringType()), True),
            StructField("Composer", ArrayType(StringType()), True),
            StructField("Actors", ArrayType(StringType()), True)
        ])

        # Create DataFrame
        movies_df = self.spark.createDataFrame(movies_data, movies_schema)

        # Convert arrays to strings
        for col_name in ["Writer", "Producer", "Composer", "Actors"]:
            movies_df = movies_df.withColumn(
                col_name,
                concat_ws(", ", col(col_name))
            )

        return movies_df

    def stop_spark(self):
        self.spark.stop()
