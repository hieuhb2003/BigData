# from pyspark.sql import DataFrame
# from pyspark.sql.functions import (
#     from_unixtime, hour, dayofweek, 
#     count, avg, max, min, sum, col, lit,
#     from_json, concat, current_timestamp, when, udf
# )
# from pyspark.sql.types import (
#     StructType, StructField, StringType, IntegerType, 
#     TimestampType, ArrayType, FloatType, BooleanType
# )
# from pyspark.sql.window import Window
# from pyspark.sql.functions import rank, first
# import spacy
# import logging

# class RedditTransformer:
#     def __init__(self, spark):
#         self.spark = spark
        
#         # Load English language model
#         self.nlp = spacy.load("en_core_web_sm")
        
#         # PostgreSQL configuration - updated with your settings
#         self.db_config = {
#             "url": "jdbc:postgresql://postgres:5432/reddit-data",
#             "user": "postgres",
#             "password": "postgres",
#             "driver": "org.postgresql.Driver"
#         }
        
#         # Schema remains the same
#         self.reddit_schema = StructType([
#             StructField("id", StringType()),
#             StructField("title", StringType()),
#             StructField("author", StringType()),
#             StructField("post_time", FloatType()),
#             StructField("upvotes", IntegerType()),
#             StructField("downvotes", IntegerType()),
#             StructField("num_comments", IntegerType()),
#             StructField("score", IntegerType()),
#             StructField("selftext", StringType()),
#             StructField("first_level_comments_count", IntegerType()),
#             StructField("second_level_comments_count", IntegerType()),
#             StructField("text", StringType()),
#             StructField("subreddit", StringType()),
#             StructField("processing_timestamp", StringType())
#         ])

#     def transform_reddit_posts(self, df: DataFrame) -> DataFrame:
#         """Transform Reddit posts data"""
#         return df.select(
#             col("id").alias("post_id"),
#             "title",
#             "author",
#             from_unixtime("post_time").alias("post_time"),
#             "upvotes",
#             "downvotes",
#             "num_comments",
#             "score",
#             "text",
#             "first_level_comments_count",
#             "second_level_comments_count",
#             "subreddit",
#             hour(from_unixtime("post_time")).alias("hour_of_day"),
#             dayofweek(from_unixtime("post_time")).alias("day_of_week"),
#             current_timestamp().alias("created_at")
#         )

#     def extract_entities(self, text):
#         """Extract entities using spaCy - UDF helper function"""
#         doc = self.nlp(text)
#         entities = []
#         for ent in doc.ents:
#             if ent.label_ in ['PERSON', 'ORG']:
#                 entities.append((ent.text, ent.label_))
#         return entities

#     def process_entities(self, posts_df: DataFrame) -> DataFrame:
#         """Process entities from title only"""
#         # Convert DataFrame to python list to process on driver
#         titles_list = [(row.post_id, row.title) for row in posts_df.select("post_id", "title").collect()]
        
#         # Process entities on driver
#         entities_list = []
#         for post_id, title in titles_list:
#             doc = self.nlp(title)
#             for ent in doc.ents:
#                 if ent.label_ in ['PERSON', 'ORG']:
#                     entities_list.append((
#                         post_id,
#                         ent.text,
#                         ent.label_,
#                         1,  # mention_count
#                     ))
        
#         # Create DataFrame from processed entities
#         entities_schema = StructType([
#             StructField("post_id", StringType(), True),
#             StructField("entity_name", StringType(), True),
#             StructField("entity_type", StringType(), True),
#             StructField("mention_count", IntegerType(), True)
#         ])
        
#         if entities_list:
#             entities_df = self.spark.createDataFrame(entities_list, entities_schema)
#             return entities_df.select(
#                 "post_id",
#                 "entity_name",
#                 "entity_type",
#                 "mention_count",
#                 current_timestamp().alias("created_at")
#             )
#         else:
#             # Return empty DataFrame with correct schema
#             return self.spark.createDataFrame(
#                 [],
#                 StructType([
#                     StructField("post_id", StringType(), True),
#                     StructField("entity_name", StringType(), True),
#                     StructField("entity_type", StringType(), True),
#                     StructField("mention_count", IntegerType(), True),
#                     StructField("created_at", TimestampType(), True)
#                 ])
#             )


#     def calculate_player_mentions(self, entities_df: DataFrame, posts_df: DataFrame) -> DataFrame:
#         """Calculate player mention statistics"""
#         # First join and filter
#         joined_df = entities_df.filter(col("entity_type") == "PERSON") \
#             .join(posts_df, "post_id")
        
#         # Create window specs for finding highest and lowest scored posts
#         window_spec = Window.partitionBy("entity_name").orderBy(col("score").desc())
#         window_spec_asc = Window.partitionBy("entity_name").orderBy(col("score").asc())
        
#         # Add rank columns
#         ranked_df = joined_df.withColumn("rank_desc", rank().over(window_spec)) \
#             .withColumn("rank_asc", rank().over(window_spec_asc))
        
#         # Get highest and lowest scored posts
#         highest_posts = ranked_df.filter(col("rank_desc") == 1) \
#             .select("entity_name", "post_id").alias("highest")
#         lowest_posts = ranked_df.filter(col("rank_asc") == 1) \
#             .select("entity_name", "post_id").alias("lowest")
        
#         # Calculate aggregate metrics
#         metrics_df = joined_df.groupBy("entity_name") \
#             .agg(
#                 count("post_id").alias("total_posts"),
#                 sum("mention_count").alias("total_mentions"),
#                 sum("upvotes").alias("total_upvotes"),
#                 avg("score").alias("avg_post_score"),
#                 current_timestamp().alias("last_updated")
#             )
        
#         # Join everything together
#         return metrics_df \
#             .join(highest_posts, "entity_name") \
#             .withColumnRenamed("post_id", "highest_upvoted_post_id") \
#             .join(lowest_posts, "entity_name") \
#             .withColumnRenamed("post_id", "lowest_upvoted_post_id")

#     def calculate_time_engagement(self, df: DataFrame) -> DataFrame:
#         """Calculate engagement statistics by time"""
#         return df.groupBy("hour_of_day", "day_of_week") \
#             .agg(
#                 avg("upvotes").alias("avg_upvotes"),
#                 avg("num_comments").alias("avg_comments"),
#                 count("*").alias("total_posts"),
#                 current_timestamp().alias("last_updated")
#             )

#     def save_to_postgres(self, df: DataFrame, table_name: str, mode: str = "append"):
#         """Save DataFrame to PostgreSQL"""
#         df.write \
#             .format("jdbc") \
#             .option("url", self.db_config["url"]) \
#             .option("dbtable", table_name) \
#             .option("user", self.db_config["user"]) \
#             .option("password", self.db_config["password"]) \
#             .option("driver", self.db_config["driver"]) \
#             .mode(mode) \
#             .save()

from pyspark.sql import DataFrame
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class RedditTransformer:
    def __init__(self, spark):
        """Initialize RedditTransformer with Spark session and configurations"""
        self.spark = spark
        
        # Load English language model
        try:
            logger.info("Loading spaCy model...")
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("Successfully loaded spaCy model")
        except Exception as e:
            logger.error(f"Failed to load spaCy model: {str(e)}")
            raise e
        
        # PostgreSQL configuration from environment variables
        try:
            postgres_host = os.getenv('POSTGRES_HOST', 'host.docker.internal')
            postgres_port = os.getenv('POSTGRES_PORT', '5432')
            postgres_db = os.getenv('POSTGRES_DB', 'reddit-data')
            postgres_user = os.getenv('POSTGRES_USER', 'postgres')
            postgres_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
            
            self.db_config = {
                "url": f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}",
                "user": postgres_user,
                "password": postgres_password,
                "driver": "org.postgresql.Driver"
            }
            logger.info(f"Initialized database configuration with host: {postgres_host}")
        except Exception as e:
            logger.error(f"Error initializing database configuration: {str(e)}")
            raise e
        
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
        logger.info("Schema initialized")

    def transform_reddit_posts(self, df: DataFrame) -> DataFrame:
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

        
    def process_entities(self, posts_df: DataFrame) -> DataFrame:
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

    def calculate_player_mentions(self, entities_df: DataFrame, posts_df: DataFrame) -> DataFrame:
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

    def calculate_time_engagement(self, df: DataFrame) -> DataFrame:
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
        
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import (
#     from_unixtime, hour, dayofweek, 
#     count, avg, max, min, sum, col, lit,
#     from_json, concat, current_timestamp, when, udf
# )
# from pyspark.sql.types import (
#     StructType, StructField, StringType, IntegerType, 
#     TimestampType, ArrayType, FloatType, BooleanType
# )
# from pyspark.sql.window import Window
# from pyspark.sql.functions import rank, first
# import spacy
# import logging
# import os
# import time

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s] %(message)s'
# )
# logger = logging.getLogger(__name__)

# class RedditTransformer:
#     def __init__(self, spark):
#         """Initialize RedditTransformer with Spark session and configurations"""
#         self.spark = spark
        
#         # Load English language model
#         try:
#             logger.info("Loading spaCy model...")
#             self.nlp = spacy.load("en_core_web_sm")
#             logger.info("Successfully loaded spaCy model")
#         except Exception as e:
#             logger.error(f"Failed to load spaCy model: {str(e)}")
#             raise e
        
#         # PostgreSQL configuration from environment variables
#         try:
#             postgres_host = os.getenv('POSTGRES_HOST', 'host.docker.internal')
#             postgres_port = os.getenv('POSTGRES_PORT', '5432')
#             postgres_db = os.getenv('POSTGRES_DB', 'reddit-data')
#             postgres_user = os.getenv('POSTGRES_USER', 'postgres')
#             postgres_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
            
#             self.db_config = {
#                 "url": f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}",
#                 "user": postgres_user,
#                 "password": postgres_password,
#                 "driver": "org.postgresql.Driver"
#             }
#             logger.info(f"Initialized database configuration with host: {postgres_host}")
#         except Exception as e:
#             logger.error(f"Error initializing database configuration: {str(e)}")
#             raise e
        
#         # Schema definition
#         self.reddit_schema = StructType([
#             StructField("id", StringType()),
#             StructField("title", StringType()),
#             StructField("author", StringType()),
#             StructField("post_time", FloatType()),
#             StructField("upvotes", IntegerType()),
#             StructField("downvotes", IntegerType()),
#             StructField("num_comments", IntegerType()),
#             StructField("score", IntegerType()),
#             StructField("selftext", StringType()),
#             StructField("first_level_comments_count", IntegerType()),
#             StructField("second_level_comments_count", IntegerType()),
#             StructField("text", StringType()),
#             StructField("subreddit", StringType()),
#             StructField("processing_timestamp", StringType())
#         ])
#         logger.info("Schema initialized")

#     def transform_reddit_posts(self, df: DataFrame) -> DataFrame:
#         """Transform Reddit posts data"""
#         try:
#             logger.info("Starting posts transformation...")
#             transformed_df = df.select(
#                 col("id").alias("post_id"),
#                 "title",
#                 "author",
#                 from_unixtime(col("post_time")).cast("timestamp").alias("post_time"),
#                 "upvotes",
#                 "downvotes",
#                 "num_comments",
#                 "score",
#                 "text",
#                 "first_level_comments_count",
#                 "second_level_comments_count",
#                 "subreddit",
#                 hour(from_unixtime("post_time")).alias("hour_of_day"),
#                 dayofweek(from_unixtime("post_time")).alias("day_of_week"),
#                 current_timestamp().alias("created_at")
#             )
#             row_count = transformed_df.count()
#             logger.info(f"Successfully transformed {row_count} posts")
#             return transformed_df
#         except Exception as e:
#             logger.error(f"Error transforming posts: {str(e)}")
#             raise e
        
#     def process_entities(self, posts_df: DataFrame) -> DataFrame:
#         """Process entities from title only"""
#         try:
#             logger.info("Starting entity extraction from posts...")
            
#             titles_list = [(row.post_id, row.title) for row in posts_df.select("post_id", "title").collect()]
#             logger.info(f"Processing entities for {len(titles_list)} titles")
            
#             entities_list = []
#             for post_id, title in titles_list:
#                 doc = self.nlp(title)
#                 for ent in doc.ents:
#                     if ent.label_ in ['PERSON', 'ORG']:
#                         entities_list.append((
#                             post_id,
#                             ent.text,
#                             ent.label_,
#                             1,  # mention_count
#                         ))
            
#             logger.info(f"Found {len(entities_list)} entities")
            
#             entities_schema = StructType([
#                 StructField("post_id", StringType(), True),
#                 StructField("entity_name", StringType(), True),
#                 StructField("entity_type", StringType(), True),
#                 StructField("mention_count", IntegerType(), True)
#             ])
            
#             if entities_list:
#                 entities_df = self.spark.createDataFrame(entities_list, entities_schema)
#                 result_df = entities_df.select(
#                     "post_id",
#                     "entity_name",
#                     "entity_type",
#                     "mention_count",
#                     current_timestamp().alias("created_at")
#                 )
#                 logger.info(f"Successfully created entities DataFrame with {result_df.count()} rows")
#                 return result_df
#             else:
#                 logger.info("No entities found, returning empty DataFrame")
#                 return self.spark.createDataFrame(
#                     [],
#                     StructType([
#                         StructField("post_id", StringType(), True),
#                         StructField("entity_name", StringType(), True),
#                         StructField("entity_type", StringType(), True),
#                         StructField("mention_count", IntegerType(), True),
#                         StructField("created_at", TimestampType(), True)
#                     ])
#                 )
#         except Exception as e:
#             logger.error(f"Error processing entities: {str(e)}")
#             raise e

#     def calculate_player_mentions(self, entities_df: DataFrame, posts_df: DataFrame) -> DataFrame:
#         """Calculate player mention statistics"""
#         try:
#             logger.info("Starting player mentions calculation...")
            
#             joined_df = entities_df.filter(col("entity_type") == "PERSON") \
#                 .join(posts_df, "post_id")
            
#             joined_count = joined_df.count()
#             logger.info(f"Found {joined_count} player mentions")
            
#             if joined_count == 0:
#                 logger.info("No player mentions found, returning empty DataFrame")
#                 return self.spark.createDataFrame([], 
#                     StructType([
#                         StructField("player_name", StringType(), True),
#                         StructField("total_mentions", IntegerType(), True),
#                         StructField("total_posts", IntegerType(), True),
#                         StructField("total_upvotes", IntegerType(), True),
#                         StructField("avg_post_score", FloatType(), True),
#                         StructField("highest_upvoted_post_id", StringType(), True),
#                         StructField("lowest_upvoted_post_id", StringType(), True),
#                         StructField("last_updated", TimestampType(), True)
#                     ]))
            
#             window_spec = Window.partitionBy("entity_name").orderBy(col("score").desc())
#             window_spec_asc = Window.partitionBy("entity_name").orderBy(col("score").asc())
            
#             ranked_df = joined_df.withColumn("rank_desc", rank().over(window_spec)) \
#                 .withColumn("rank_asc", rank().over(window_spec_asc))
            
#             highest_posts = ranked_df.filter(col("rank_desc") == 1) \
#                 .select("entity_name", "post_id").alias("highest")
#             lowest_posts = ranked_df.filter(col("rank_asc") == 1) \
#                 .select("entity_name", "post_id").alias("lowest")
            
#             metrics_df = joined_df.groupBy("entity_name") \
#                 .agg(
#                     sum("mention_count").alias("total_mentions"),
#                     count("post_id").alias("total_posts"),
#                     sum("upvotes").alias("total_upvotes"),
#                     avg("score").alias("avg_post_score"),
#                     current_timestamp().alias("last_updated")
#                 )
            
#             result_df = metrics_df \
#                 .join(highest_posts, "entity_name") \
#                 .withColumnRenamed("post_id", "highest_upvoted_post_id") \
#                 .join(lowest_posts, "entity_name") \
#                 .withColumnRenamed("post_id", "lowest_upvoted_post_id") \
#                 .withColumnRenamed("entity_name", "player_name")
            
#             logger.info(f"Successfully calculated mentions for {result_df.count()} players")
#             return result_df
                
#         except Exception as e:
#             logger.error(f"Error calculating player mentions: {str(e)}")
#             raise e

#     def calculate_time_engagement(self, df: DataFrame) -> DataFrame:
#         """Calculate engagement statistics by time"""
#         try:
#             logger.info("Starting time engagement calculation...")
#             result_df = df.groupBy("hour_of_day", "day_of_week") \
#                 .agg(
#                     avg("upvotes").alias("avg_upvotes"),
#                     avg("num_comments").alias("avg_comments"),
#                     count("*").alias("total_posts"),
#                     current_timestamp().alias("last_updated")
#                 )
            
#             logger.info(f"Calculated engagement metrics for {result_df.count()} time periods")
#             return result_df
#         except Exception as e:
#             logger.error(f"Error calculating time engagement: {str(e)}")
#             raise e

#     def save_to_postgres(self, df: DataFrame, table_name: str, mode: str = "append", max_retries: int = 3):
#         """
#         Save DataFrame to PostgreSQL with improved error handling and retries
        
#         Args:
#             df: DataFrame to save
#             table_name: Target table name
#             mode: Save mode (append/overwrite/ignore)
#             max_retries: Maximum number of retry attempts
#         """
#         try:
#             row_count = df.count()
#             logger.info(f"Attempting to save {row_count} rows to table {table_name}")
            
#             if row_count == 0:
#                 logger.info(f"Skipping save for empty DataFrame to table {table_name}")
#                 return

#             # Validate data before saving
#             self._validate_data_for_table(df, table_name)
            
#             # Log sample data for debugging
#             logger.info(f"Sample data for table {table_name}:")
#             df.show(2, truncate=False)

#             # Determine save mode based on table
#             save_mode = self._get_save_mode(table_name, mode)
            
#             # Initialize retry counter
#             retry_count = 0
#             last_error = None
            
#             while retry_count < max_retries:
#                 try:
#                     # Add batch_id and timestamp for tracking
#                     df_with_metadata = df.withColumn(
#                         "inserted_at", 
#                         current_timestamp()
#                     )
                    
#                     # Configure JDBC properties
#                     jdbc_properties = {
#                         "url": self.db_config["url"],
#                         "dbtable": table_name,
#                         "user": self.db_config["user"],
#                         "password": self.db_config["password"],
#                         "driver": self.db_config["driver"],
#                         # Additional optimizations
#                         "batchsize": "10000",
#                         "rewriteBatchedStatements": "true",
#                         "isolationLevel": "REPEATABLE_READ"
#                     }
                    
#                     df_with_metadata.write \
#                         .format("jdbc") \
#                         .options(**jdbc_properties) \
#                         .mode(save_mode) \
#                         .save()
                    
#                     logger.info(f"Successfully saved {row_count} rows to table {table_name}")
#                     return
                    
#                 except Exception as e:
#                     last_error = e
#                     retry_count += 1
                    
#                     if retry_count < max_retries:
#                         wait_time = 2 ** retry_count  # Exponential backoff
#                         logger.warning(
#                             f"Attempt {retry_count} failed for table {table_name}. "
#                             f"Retrying in {wait_time} seconds... Error: {str(e)}"
#                         )
#                         time.sleep(wait_time)
#                     else:
#                         break
                        
#             # If all retries failed
#             raise Exception(
#                 f"Failed to save to PostgreSQL after {max_retries} attempts. "
#                 f"Last error: {str(last_error)}"
#             )
                    
#         except Exception as e:
#             logger.error(
#                 f"Failed to save to PostgreSQL table {table_name}: {str(e)}", 
#                 exc_info=True
#             )
#             raise e

#     def _validate_data_for_table(self, df: DataFrame, table_name: str):
#         """Validate DataFrame schema and data based on table requirements"""
#         try:
#             # Define expected schemas for each table
#             expected_schemas = {
#                 "reddit_posts": ["post_id", "title", "author", "post_time"],
#                 "post_entities": ["post_id", "entity_name", "entity_type"],
#                 "player_mentions_stats": ["player_name", "total_mentions"],
#                 "time_engagement_stats": ["hour_of_day", "day_of_week"]
#             }
            
#             # Check if table has defined schema
#             if table_name not in expected_schemas:
#                 logger.warning(f"No schema validation defined for table {table_name}")
#                 return
                
#             # Validate required columns exist
#             required_columns = expected_schemas[table_name]
#             missing_columns = [col for col in required_columns if col not in df.columns]
            
#             if missing_columns:
#                 raise ValueError(
#                     f"Missing required columns for table {table_name}: {missing_columns}"
#                 )
                
#             # Validate no null values in key columns
#             for column in required_columns:
#                 null_count = df.filter(col(column).isNull()).count()
#                 if null_count > 0:
#                     raise ValueError(
#                         f"Found {null_count} null values in required column {column}"
#                     )
                    
#             logger.info(f"Data validation passed for table {table_name}")
            
#         except Exception as e:
#             logger.error(f"Data validation failed: {str(e)}")
#             raise e

#     def _get_save_mode(self, table_name: str, default_mode: str) -> str:
#         """Determine appropriate save mode based on table name"""
#         table_modes = {
#             "reddit_posts": "ignore",  # Skip if exists
#             "post_entities": "append",  # Always append
#             "player_mentions_stats": "overwrite",  # Refresh stats
#             "time_engagement_stats": "overwrite"  # Refresh stats
#         }
#         return table_modes.get(table_name, default_mode)