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
        
        try:
            logger.info("Loading spaCy model...")
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("Successfully loaded spaCy model")
        except Exception as e:
            logger.error(f"Failed to load spaCy model: {str(e)}")
            raise e
        
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
                        StructField("player_name", StringType(), True),
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
                .withColumnRenamed("entity_name", "player_name") 
            
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

    def save_to_postgres(self, df, table_name: str):
        """Save DataFrame to PostgreSQL with duplicate handling"""
        try:
            row_count = df.count()
            logger.info(f"Attempting to save {row_count} rows to table {table_name}")
            
            if row_count == 0:
                logger.info(f"Skipping save for empty DataFrame to table {table_name}")
                return

            # Xử lý theo từng loại bảng
            if table_name == "reddit_posts":
                df.createOrReplaceTempView("temp_posts")
                
                # Sử dụng INSERT ON CONFLICT
                df.write \
                    .format("jdbc") \
                    .option("url", self.db_config["url"]) \
                    .option("dbtable", table_name) \
                    .option("user", self.db_config["user"]) \
                    .option("password", self.db_config["password"]) \
                    .option("driver", self.db_config["driver"]) \
                    .option("batchsize", 1000) \
                    .option("isolationLevel", "REPEATABLE_READ") \
                    .mode("append") \
                    .option("insertStatement", """
                        INSERT INTO reddit_posts (
                            post_id, title, author, post_time, upvotes, 
                            downvotes, num_comments, score, text, 
                            first_level_comments_count, second_level_comments_count,
                            subreddit, hour_of_day, day_of_week, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (post_id) DO NOTHING
                    """) \
                    .save()
                    
            elif table_name == "post_entities":
                # Check foreign key và insert ignore
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
                
                valid_entities = df.join(existing_posts, "post_id", "inner")
                
                if valid_entities.count() == 0:
                    logger.warning("No valid entities found with existing post_ids")
                    return
                    
                valid_entities.write \
                    .format("jdbc") \
                    .option("url", self.db_config["url"]) \
                    .option("dbtable", table_name) \
                    .option("user", self.db_config["user"]) \
                    .option("password", self.db_config["password"]) \
                    .option("driver", self.db_config["driver"]) \
                    .mode("append") \
                    .option("insertStatement", """
                        INSERT INTO post_entities (
                            post_id, entity_name, entity_type, mention_count, created_at
                        ) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (post_id, entity_name) DO NOTHING
                    """) \
                    .save()
                    
            else:
                # Các bảng statistics có thể overwrite
                df.write \
                    .format("jdbc") \
                    .option("url", self.db_config["url"]) \
                    .option("dbtable", table_name) \
                    .option("user", self.db_config["user"]) \
                    .option("password", self.db_config["password"]) \
                    .option("driver", self.db_config["driver"]) \
                    .mode("overwrite") \
                    .save()
            
            logger.info(f"Successfully saved {row_count} rows to table {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to save to PostgreSQL table {table_name}: {str(e)}", exc_info=True)
            raise e        
