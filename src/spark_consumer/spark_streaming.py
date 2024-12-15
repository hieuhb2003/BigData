# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from transformation import RedditTransformer
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def create_spark_session():
#     """Create Spark session with required configurations"""
#     return SparkSession.builder \
#         .appName("RedditSoccerAnalysis") \
#         .getOrCreate()

# def process_stream():
#     """Main function to process streaming data"""
#     spark = create_spark_session()
#     transformer = RedditTransformer(spark)
    
#     # Read from Kafka
#     kafka_df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "kafka:9092") \
#         .option("subscribe", "reddit_data") \
#         .option("startingOffsets", "earliest") \
#         .load()

#     # Parse JSON data
#     parsed_df = kafka_df.select(
#         from_json(
#             col("value").cast("string"),
#             transformer.reddit_schema
#         ).alias("data")
#     ).select("data.*")

#     def process_batch(batch_df, batch_id):
#         try:
#             if batch_df.count() == 0:
#                 logger.info(f"Empty batch {batch_id}")
#                 return

#             # Transform posts
#             posts_df = transformer.transform_reddit_posts(batch_df)
#             transformer.save_to_postgres(posts_df, "reddit_posts")

#             # Extract and save entities
#             entities_df = transformer.process_entities(posts_df)
#             if not entities_df.isEmpty():
#                 transformer.save_to_postgres(entities_df, "post_entities")

#             # Calculate and save player mentions
#             if not entities_df.isEmpty():
#                 player_mentions_df = transformer.calculate_player_mentions(entities_df, posts_df)
#                 transformer.save_to_postgres(player_mentions_df, "player_mentions_stats")

#             # Calculate and save time engagement
#             time_engagement_df = transformer.calculate_time_engagement(posts_df)
#             transformer.save_to_postgres(time_engagement_df, "time_engagement_stats")

#             logger.info(f"Successfully processed batch {batch_id}")
#         except Exception as e:
#             logger.error(f"Error processing batch {batch_id}: {e}")
#             raise e

#     # Process the stream
#     streaming_query = parsed_df.writeStream \
#         .foreachBatch(process_batch) \
#         .outputMode("update") \
#         .trigger(processingTime="1 minute") \
#         .start()

#     return streaming_query

# def main():
#     try:
#         logger.info("Starting Spark Streaming job")
#         query = process_stream()
#         query.awaitTermination()
#     except Exception as e:
#         logger.error(f"Error in main: {e}")
#         raise e

# if __name__ == "__main__":
#     main()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from transformation import RedditTransformer
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def create_spark_session():
#     """Create Spark session with required configurations"""
#     return SparkSession.builder \
#         .appName("RedditSoccerAnalysis") \
#         .getOrCreate()

# def process_stream():
#     """Main function to process streaming data"""
#     logger.info("Starting stream processing with Kafka broker: kafka:9092")
    
#     spark = create_spark_session()
#     transformer = RedditTransformer(spark)

#     # Log expected schema
#     logger.info(f"Expected schema: {transformer.reddit_schema}")

#     # Read from Kafka with enhanced configurations
#     kafka_df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "kafka:9092") \
#         .option("subscribe", "reddit_data") \
#         .option("startingOffsets", "earliest") \
#         .option("failOnDataLoss", "false") \
#         .option("maxOffsetsPerTrigger", 1000) \
#         .load()

#     # Log Kafka schema
#     logger.info("Kafka raw schema:")
#     kafka_df.printSchema()

#     # Parse JSON data
#     parsed_df = kafka_df.select(
#         from_json(
#             col("value").cast("string"),
#             transformer.reddit_schema
#         ).alias("data")
#     ).select("data.*")

#     def process_batch(batch_df, batch_id):
#         try:
#             count = batch_df.count()
#             logger.info(f"Processing batch {batch_id} with {count} records")
            
#             if count == 0:
#                 logger.info(f"Empty batch {batch_id}")
#                 return

#             # Log schema and sample data
#             logger.info("Batch data schema:")
#             batch_df.printSchema()
#             logger.info("Sample data:")
#             batch_df.show(5, truncate=False)

#             # Transform posts
#             posts_df = transformer.transform_reddit_posts(batch_df)
#             logger.info(f"Transformed {posts_df.count()} posts")
#             transformer.save_to_postgres(posts_df, "reddit_posts")

#             # Extract and save entities
#             entities_df = transformer.process_entities(posts_df)
#             if not entities_df.isEmpty():
#                 logger.info(f"Processed {entities_df.count()} entities")
#                 transformer.save_to_postgres(entities_df, "post_entities")

#                 # Calculate and save player mentions
#                 player_mentions_df = transformer.calculate_player_mentions(entities_df, posts_df)
#                 transformer.save_to_postgres(player_mentions_df, "player_mentions_stats")

#             # Calculate and save time engagement
#             time_engagement_df = transformer.calculate_time_engagement(posts_df)
#             transformer.save_to_postgres(time_engagement_df, "time_engagement_stats")

#             logger.info(f"Successfully processed batch {batch_id}")

#         except Exception as e:
#             logger.error(f"Error processing batch {batch_id}: {str(e)}", exc_info=True)
#             raise e

#     # Process the stream
#     streaming_query = parsed_df.writeStream \
#         .foreachBatch(process_batch) \
#         .outputMode("update") \
#         .trigger(processingTime="1 minute") \
#         .start()

#     return streaming_query

# def main():
#     try:
#         logger.info("Starting Spark Streaming job")
#         query = process_stream()
#         query.awaitTermination()
#     except Exception as e:
#         logger.error(f"Error in main: {e}", exc_info=True)
#         raise e

# if __name__ == "__main__":
#     main()

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
        logger.info("Setting up Kafka stream reader with broker: kafka:9092")
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "reddit_data") \
            .option("startingOffsets", "earliest") \
            .option("kafka.group.id", "reddit_processor_group") \
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
            """Process each batch of data"""
            try:
                count = batch_df.count()
                logger.info(f"Starting to process batch {batch_id} with {count} records")
                
                if count == 0:
                    logger.info(f"Empty batch {batch_id}, skipping processing")
                    return

                # Validate
                invalid_records = batch_df.filter(
                    col("id").isNull() | 
                    col("post_time").isNull() |
                    col("author").isNull()
                )
                
                if invalid_records.count() > 0:
                    logger.warning(f"Found {invalid_records.count()} invalid records in batch {batch_id}")
                    batch_df = batch_df.filter(
                        col("id").isNotNull() & 
                        col("post_time").isNotNull() &
                        col("author").isNotNull()
                    )

                # Drop duplicates và persist
                deduplicated_df = batch_df.dropDuplicates(["id"])
                deduplicated_df.persist()  # Cache lại để tái sử dụng
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