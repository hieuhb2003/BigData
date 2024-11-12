# from src.crawlers.crawl import get_movie_data
from src.data_ingestion.minio_uploader import MinioUploader
from src.data_transformation.data_processor import DataProcessor
from src.data_loading.warehouse_loader import WarehouseLoader
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # logger.info("Starting data crawling...")
        # get_movie_data()

        logger.info("Uploading data to MinIO...")
        uploader = MinioUploader('config/config.yaml')
        uploader.upload_json('Bigdata.json', 'raw/rankings.json')
        uploader.upload_json('film.json', 'raw/movies.json')

        logger.info("Processing data...")
        processor = DataProcessor()
        
        with open('Bigdata.json', 'r') as f:
            rankings_data = json.load(f)
        with open('film.json', 'r') as f:
            movies_data = json.load(f)

        rankings_df = processor.process_rankings(rankings_data)
        movies_df = processor.process_movies(movies_data)

        logger.info("Loading data to warehouse...")
        loader = WarehouseLoader('config/config.yaml')
        loader.load_rankings(rankings_df)
        loader.load_movies(movies_df)

        logger.info("ETL process completed successfully!")

    except Exception as e:
        logger.error(f"Error during ETL process: {str(e)}")
        raise e

    finally:
        if 'processor' in locals():
            processor.stop_spark()

if __name__ == "__main__":
    main()
