from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_unixtime, hour, dayofweek, 
    count, avg, max, min, sum, col, lit,
    from_json, concat, current_timestamp, when, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, ArrayType, FloatType, BooleanType
)
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, first
import spacy
import logging
import os
import json
import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Sample Reddit data
sample_data = [
    {
        "id": "post1",
        "title": "Erling Haaland scores hat-trick against Manchester United",
        "author": "soccer_fan_123",
        "post_time": 1702591200.0,  # 1 hour ago
        "upvotes": 1500,
        "downvotes": 100,
        "num_comments": 234,
        "score": 1400,
        "selftext": "Amazing performance by Haaland today!",
        "first_level_comments_count": 150,
        "second_level_comments_count": 84,
        "text": "Amazing performance by Haaland today!",
        "subreddit": "soccer",
        "processing_timestamp": datetime.datetime.now().isoformat()
    },
    {
        "id": "post2",
        "title": "Marcus Rashford discusses Manchester United's tactics",
        "author": "red_devil",
        "post_time": 1702598400.0,  # 2 hours ago
        "upvotes": 800,
        "downvotes": 50,
        "num_comments": 156,
        "score": 750,
        "selftext": "Interesting insight from Rashford about the team's approach",
        "first_level_comments_count": 100,
        "second_level_comments_count": 56,
        "text": "Interesting insight from Rashford about the team's approach",
        "subreddit": "soccer",
        "processing_timestamp": datetime.datetime.now().isoformat()
    }
]

class RedditTransformer:
    def __init__(self, spark):
        """Initialize RedditTransformer with Spark session and configurations"""
        self.spark = spark
        
        try:
            logger.info("Loading spaCy model...")
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("Successfully loaded spaCy model")
        except Exception as e:
            logger.error(f"Failed to load spaCy model: {str(e)}")
            raise e
        
        # PostgreSQL configuration
        self.db_config = {
            "url": "jdbc:postgresql://localhost:5432/reddit-data",
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }
        
        # Schema definition
        self.reddit_schema = StructType([
            StructField("id", StringType()),
            StructField("title", StringType()),
            StructField("author", StringType()),
            StructField("post_time", FloatType()),
            StructField("upvotes", IntegerType()),
            StructField("downvotes", IntegerType()),
            StructField("num_comments", IntegerType()),
            StructField("score", IntegerType()),
            StructField("selftext", StringType()),
            StructField("first_level_comments_count", IntegerType()),
            StructField("second_level_comments_count", IntegerType()),
            StructField("text", StringType()),
            StructField("subreddit", StringType()),
            StructField("processing_timestamp", StringType())
        ])
    def transform_reddit_posts(self, df):
        """Transform Reddit posts data"""
        try:
            logger.info("Starting posts transformation...")
            transformed_df = df.select(
                col("id").alias("post_id"),
                "title",
                "author",
                from_unixtime(col("post_time")).cast("timestamp").alias("post_time"),
                "upvotes",
                "downvotes",
                "num_comments",
                "score",
                "text",
                "first_level_comments_count",
                "second_level_comments_count",
                "subreddit",
                hour(from_unixtime("post_time")).alias("hour_of_day"),
                dayofweek(from_unixtime("post_time")).alias("day_of_week"),
                current_timestamp().alias("created_at")
            )
            # Cache DataFrame để tránh tính toán lại
            transformed_df.persist()
            row_count = transformed_df.count()
            logger.info(f"Successfully transformed {row_count} posts")
            return transformed_df
        except Exception as e:
            logger.error(f"Error transforming posts: {str(e)}")
            raise e

        
    def process_entities(self, posts_df):
        """Process entities from title only"""
        try:
            logger.info("Starting entity extraction from posts...")
            
            # Join với posts_df để chỉ lấy entities từ posts đã tồn tại
            valid_posts = posts_df.select("post_id", "title")
            valid_posts.persist()
            
            titles_list = [(row.post_id, row.title) for row in valid_posts.collect()]
            logger.info(f"Processing entities for {len(titles_list)} titles")
            
            entities_list = []
            for post_id, title in titles_list:
                doc = self.nlp(title)
                for ent in doc.ents:
                    if ent.label_ in ['PERSON', 'ORG']:
                        entities_list.append((
                            post_id,
                            ent.text,
                            ent.label_,
                            1,  # mention_count
                        ))
            
            logger.info(f"Found {len(entities_list)} entities")
            valid_posts.unpersist()

            if entities_list:
                entities_schema = StructType([
                    StructField("post_id", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("entity_type", StringType(), True),
                    StructField("mention_count", IntegerType(), True)
                ])
                
                entities_df = self.spark.createDataFrame(entities_list, entities_schema)
                result_df = entities_df.select(
                    "post_id",
                    "entity_name",
                    "entity_type",
                    "mention_count",
                    current_timestamp().alias("created_at")
                )
                return result_df
            else:
                return self.spark.createDataFrame([], StructType([
                    StructField("post_id", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("entity_type", StringType(), True),
                    StructField("mention_count", IntegerType(), True),
                    StructField("created_at", TimestampType(), True)
                ]))
        except Exception as e:
            logger.error(f"Error processing entities: {str(e)}")
            raise e

    def calculate_player_mentions(self, entities_df, posts_df):
        """Calculate player mention statistics"""
        try:
            logger.info("Starting player mentions calculation...")
            
            # First join and filter
            joined_df = entities_df.filter(col("entity_type") == "PERSON") \
                .join(posts_df, "post_id")
            
            joined_count = joined_df.count()
            logger.info(f"Found {joined_count} player mentions")
            
            if joined_count == 0:
                logger.info("No player mentions found, returning empty DataFrame")
                return self.spark.createDataFrame([], 
                    StructType([
                        StructField("player_name", StringType(), True),  # Changed from entity_name
                        StructField("total_mentions", IntegerType(), True),
                        StructField("total_posts", IntegerType(), True),
                        StructField("total_upvotes", IntegerType(), True),
                        StructField("avg_post_score", FloatType(), True),
                        StructField("highest_upvoted_post_id", StringType(), True),
                        StructField("lowest_upvoted_post_id", StringType(), True),
                        StructField("last_updated", TimestampType(), True)
                    ]))
            
            # Create window specs
            window_spec = Window.partitionBy("entity_name").orderBy(col("score").desc())
            window_spec_asc = Window.partitionBy("entity_name").orderBy(col("score").asc())
            
            # Add rank columns
            ranked_df = joined_df.withColumn("rank_desc", rank().over(window_spec)) \
                .withColumn("rank_asc", rank().over(window_spec_asc))
            
            # Get highest and lowest scored posts
            highest_posts = ranked_df.filter(col("rank_desc") == 1) \
                .select("entity_name", "post_id").alias("highest")
            lowest_posts = ranked_df.filter(col("rank_asc") == 1) \
                .select("entity_name", "post_id").alias("lowest")
            
            # Calculate aggregate metrics
            metrics_df = joined_df.groupBy("entity_name") \
                .agg(
                    sum("mention_count").alias("total_mentions"),
                    count("post_id").alias("total_posts"),
                    sum("upvotes").alias("total_upvotes"),
                    avg("score").alias("avg_post_score"),
                    current_timestamp().alias("last_updated")
                )
            
            # Join everything together and rename entity_name to player_name
            result_df = metrics_df \
                .join(highest_posts, "entity_name") \
                .withColumnRenamed("post_id", "highest_upvoted_post_id") \
                .join(lowest_posts, "entity_name") \
                .withColumnRenamed("post_id", "lowest_upvoted_post_id") \
                .withColumnRenamed("entity_name", "player_name")  # Add this line
            
            logger.info(f"Successfully calculated mentions for {result_df.count()} players")
            return result_df
                
        except Exception as e:
            logger.error(f"Error calculating player mentions: {str(e)}")
            raise e

    def calculate_time_engagement(self, df):
        """Calculate engagement statistics by time"""
        try:
            logger.info("Starting time engagement calculation...")
            result_df = df.groupBy("hour_of_day", "day_of_week") \
                .agg(
                    avg("upvotes").alias("avg_upvotes"),
                    avg("num_comments").alias("avg_comments"),
                    count("*").alias("total_posts"),
                    current_timestamp().alias("last_updated")
                )
            
            logger.info(f"Calculated engagement metrics for {result_df.count()} time periods")
            return result_df
        except Exception as e:
            logger.error(f"Error calculating time engagement: {str(e)}")
            raise e

    def save_to_postgres(self, df, table_name: str, mode: str = "append"):
        """Save DataFrame to PostgreSQL with improved error handling"""
        try:
            row_count = df.count()
            logger.info(f"Attempting to save {row_count} rows to table {table_name}")
            
            if row_count == 0:
                logger.info(f"Skipping save for empty DataFrame to table {table_name}")
                return

            # Xác định mode phù hợp cho từng bảng
            if table_name == "reddit_posts":
                # Đổi thành append để insert dữ liệu mới
                save_mode = "append"
                
            elif table_name == "post_entities":
                # Kiểm tra foreign key constraint
                posts_df = df.select("post_id").distinct()
                existing_posts = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.db_config["url"]) \
                    .option("dbtable", "reddit_posts") \
                    .option("user", self.db_config["user"]) \
                    .option("password", self.db_config["password"]) \
                    .option("driver", self.db_config["driver"]) \
                    .load() \
                    .select("post_id")
                
                # Lọc chỉ những entities có post_id hợp lệ
                valid_entities = df.join(existing_posts, "post_id", "inner")
                if valid_entities.count() == 0:
                    logger.warning("No valid entities found with existing post_ids")
                    return
                df = valid_entities
                save_mode = "append"
                
            elif table_name in ["player_mentions_stats", "time_engagement_stats"]:
                save_mode = "overwrite"
            
            # Log mẫu dữ liệu trước khi save
            logger.info(f"Sample data for table {table_name}:")
            df.show(2, truncate=False)
            
            # Thực hiện save
            df.write \
                .format("jdbc") \
                .option("url", self.db_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", self.db_config["user"]) \
                .option("password", self.db_config["password"]) \
                .option("driver", self.db_config["driver"]) \
                .mode(save_mode) \
                .save()
            
            logger.info(f"Successfully saved {row_count} rows to table {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to save to PostgreSQL table {table_name}: {str(e)}", exc_info=True)
            raise e

    # [Previous transform_reddit_posts, process_entities, calculate_player_mentions, 
    # calculate_time_engagement, and save_to_postgres methods remain the same]
    # Including them here for completeness but keeping them identical to the original code

def main():
    """Main function to process the sample data"""
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("RedditSoccerAnalysis") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
            .master("local[*]") \
            .getOrCreate()
# Add this after creating the Spark session
        test_df = spark.createDataFrame([("test",)], ["col1"])
        try:
            test_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/reddit-data") \
                .option("dbtable", "test_table") \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            print("Database connection successful!")
        except Exception as e:
            print(f"Database connection failed: {str(e)}")
        # Initialize transformer
        transformer = RedditTransformer(spark)

        # Convert sample data to Spark DataFrame
        sample_df = spark.createDataFrame(sample_data, schema=transformer.reddit_schema)
        
        # Process the data
        try:
            # Transform posts
            posts_df = transformer.transform_reddit_posts(sample_df)
            posts_df.persist()
            posts_df.show()

            logger.info("Saving posts to PostgreSQL...")
            transformer.save_to_postgres(posts_df, "reddit_posts")
            time.sleep(5)
            # Process entities
            entities_df = transformer.process_entities(posts_df)
            entities_df.show()
            if not entities_df.isEmpty():
                logger.info("Saving entities to PostgreSQL...")
                transformer.save_to_postgres(entities_df, "post_entities")
                
                # Calculate player mentions
                player_mentions_df = transformer.calculate_player_mentions(entities_df, posts_df)
                player_mentions_df.show()
                if not player_mentions_df.isEmpty():
                    transformer.save_to_postgres(player_mentions_df, "player_mentions_stats")

            # Calculate time engagement
            time_engagement_df = transformer.calculate_time_engagement(posts_df)
            transformer.save_to_postgres(time_engagement_df, "time_engagement_stats")
            
            posts_df.unpersist()
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise e

    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise e
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()