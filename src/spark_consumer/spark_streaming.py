from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from transformation import RedditTransformer
import logging
import sys
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with required configurations"""
    try:
        logger.info("Creating Spark session...")
        spark = SparkSession.builder \
            .appName("RedditSoccerAnalysis") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        
        logger.info("Successfully created Spark session")
            
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}", exc_info=True)
        raise e

def process_stream():
    """Main function to process streaming data"""
    try:
        logger.info("Starting stream processing...")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Initialize transformer
        logger.info("Initializing RedditTransformer...")
        transformer = RedditTransformer(spark)
        logger.info(f"Expected schema: {transformer.reddit_schema}")
        logger.info(f"Database config: {transformer.db_config['url']}")

        # Read from Kafka with enhanced configurations
        logger.info("Setting up Kafka stream reader with broker: kafka1:9092")
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:9092") \
            .option("subscribe", "reddit_data") \
            .option("startingOffsets", "earliest") \
            .option("kafka.group.id", "reddit_processor_group") \
            .option("checkpointLocation", "/opt/spark/checkpoints") \
            .load()
        # Change to latest to avoid processing old data

        # Log Kafka schema and options
        logger.info("Kafka reader configuration:")
        logger.info("Raw Kafka schema:")
        kafka_df.printSchema()

        # Parse JSON data
        logger.info("Setting up JSON parsing...")
        parsed_df = kafka_df.select(
            from_json(
                col("value").cast("string"),
                transformer.reddit_schema
            ).alias("data")
        ).select("data.*")

        def process_batch(batch_df, batch_id):
            try:
                count = batch_df.count()
                logger.info(f"Starting to process batch {batch_id} with {count} records")
                
                if count == 0:
                    logger.info(f"Empty batch {batch_id}, skipping processing")
                    return

                # Lấy danh sách post_id đã có trong database
                existing_posts = spark.read \
                    .format("jdbc") \
                    .option("url", transformer.db_config["url"]) \
                    .option("dbtable", "reddit_posts") \
                    .option("user", transformer.db_config["user"]) \
                    .option("password", transformer.db_config["password"]) \
                    .option("driver", transformer.db_config["driver"]) \
                    .load() \
                    .select("post_id")

                # Lọc bỏ các records đã tồn tại trong DB
                new_records = batch_df.join(existing_posts, 
                                        batch_df.id == existing_posts.post_id, 
                                        "leftanti")
                new_count = new_records.count()
                logger.info(f"Found {new_count} new records after filtering existing posts")

                if new_count == 0:
                    logger.info("No new records to process")
                    return

                # Validate
                invalid_records = new_records.filter(
                    col("id").isNull() | 
                    col("post_time").isNull() |
                    col("author").isNull()
                )
                
                if invalid_records.count() > 0:
                    logger.warning(f"Found {invalid_records.count()} invalid records in batch {batch_id}")
                    new_records = new_records.filter(
                        col("id").isNotNull() & 
                        col("post_time").isNotNull() &
                        col("author").isNotNull()
                    )

                # Tiếp tục xử lý với new_records thay vì batch_df
                deduplicated_df = new_records.dropDuplicates(["id"])
                deduplicated_df.persist()
                dedup_count = deduplicated_df.count()
                logger.info(f"After deduplication: {dedup_count} records")

                # Transform và save posts TRƯỚC TIÊN
                posts_df = transformer.transform_reddit_posts(deduplicated_df)
                posts_df.persist()  # Cache posts_df
                logger.info(f"Transformed {posts_df.count()} posts")
                
                logger.info("Saving posts to PostgreSQL...")
                transformer.save_to_postgres(posts_df, "reddit_posts")
                logger.info("Successfully saved posts to database")

                # Xử lý entities sau khi posts đã được lưu
                logger.info("Processing entities from posts...")
                entities_df = transformer.process_entities(posts_df)
                if not entities_df.isEmpty():
                    entity_count = entities_df.count()
                    logger.info(f"Found {entity_count} entities in batch {batch_id}")
                    logger.info("Saving entities to PostgreSQL...")
                    transformer.save_to_postgres(entities_df, "post_entities")
                    logger.info("Successfully saved entities to database")

                    # Calculate và save player mentions
                    logger.info("Calculating player mentions statistics...")
                    player_mentions_df = transformer.calculate_player_mentions(entities_df, posts_df)
                    if not player_mentions_df.isEmpty():
                        mention_count = player_mentions_df.count()
                        logger.info(f"Found {mention_count} player mentions")
                        transformer.save_to_postgres(player_mentions_df, "player_mentions_stats")
                    else:
                        logger.info("No player mentions found in this batch")
                else:
                    logger.info("No entities found in this batch")

                # Time engagement metrics
                logger.info("Calculating time engagement metrics...")
                time_engagement_df = transformer.calculate_time_engagement(posts_df)
                transformer.save_to_postgres(time_engagement_df, "time_engagement_stats")
                
                # Cleanup
                deduplicated_df.unpersist()
                posts_df.unpersist()
                
                logger.info(f"Successfully completed processing batch {batch_id}")

            except Exception as e:
                logger.error(f"Error processing batch {batch_id}: {str(e)}", exc_info=True)
                raise e        # Process the stream
        logger.info("Starting streaming query...")
        query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("update") \
            .option("checkpointLocation", "/opt/spark/checkpoints") \
            .trigger(once=True) \
            .start()

        # Wait for the query to finish
        logger.info("Waiting for streaming query to complete...")
        query.awaitTermination()
        logger.info("Streaming query completed successfully")

    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}", exc_info=True)
        raise e

def main():
    """Main entry point"""
    try:
        logger.info("Starting Spark Streaming job")
        
        # Process all available data
        process_stream()
        
        logger.info("Spark Streaming job completed successfully")
    except Exception as e:
        logger.error(f"Critical error in main: {str(e)}", exc_info=True)
        raise e
    finally:
        logger.info("Cleaning up resources...")

if __name__ == "__main__":
    main()