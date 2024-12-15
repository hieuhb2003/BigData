from typing import List, Dict, Any
import json
import datetime
import logging
import time
from kafka import KafkaProducer
import kafka.errors
import praw
from prawcore.exceptions import PrawcoreException

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO, force=True)

# # Reddit API credentials - replace with your own
# REDDIT_CLIENT_ID = "your_client_id"
# REDDIT_CLIENT_SECRET = "your_client_secret"
# REDDIT_USER_AGENT = "your_user_agent"

# Kafka configuration
KAFKA_TOPIC = "reddit_data"
KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
KAFKA_LOCAL_BOOTSTRAP_SERVERS = ["localhost:9094"]

# Reddit configuration
SUBREDDITS = ["soccer", "football"]   # You can change this to specific subreddits
FETCH_LIMIT = 25  # Number of posts to fetch per iteration
SLEEP_TIME = 60  # Time to wait between iterations in seconds

def create_reddit_client() -> praw.Reddit:
    """
    Creates and returns a Reddit client instance
    """
    return praw.Reddit(
        client_id="Db5HxiXppDbz_2yVdQk8Yg",
        client_secret="bUN0iWx8_dparp05-7JQlledUx0wBA",
        user_agent="python:Streaming_Kafka_Project:1.0 (by /u/New-Deer-1312)"
    )

def create_kafka_producer() -> KafkaProducer:
    """
    Creates and returns a Kafka producer instance
    """
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    except kafka.errors.NoBrokersAvailable:
        logging.info(
            "Using localhost Kafka connection instead of container network"
        )
        producer = KafkaProducer(bootstrap_servers=KAFKA_LOCAL_BOOTSTRAP_SERVERS)
    
    return producer

def process_submission(submission: praw.models.Submission) -> Dict[str, Any]:
    """
    Processes a Reddit submission and returns a dictionary with relevant information
    """
    # Get first-level comments count
    submission.comments.replace_more(limit=0)
    first_level_comments = len(submission.comments)
    
    # Get second-level comments count
    second_level_comments = sum(
        len(comment.replies) for comment in submission.comments
        if hasattr(comment, 'replies')
    )
    
    return {
        "id": submission.id,
        "title": submission.title,
        "author": submission.author.name if submission.author else None,
        "post_time": submission.created_utc,
        "upvotes": submission.ups,
        "downvotes": submission.downs,
        "num_comments": submission.num_comments,
        "score": submission.score,
        "selftext": submission.selftext if submission.is_self else None,
        "first_level_comments_count": first_level_comments,
        "second_level_comments_count": second_level_comments,
        "text": submission.selftext,
        "subreddit": submission.subreddit.display_name,
        "processing_timestamp": datetime.datetime.now().isoformat()
    }

def get_reddit_data(reddit: praw.Reddit) -> List[Dict[str, Any]]:
    """
    Fetches recent submissions from multiple subreddits
    """
    all_submissions = []
    
    for subreddit_name in SUBREDDITS:
        try:
            subreddit = reddit.subreddit(subreddit_name)
            submissions = subreddit.new(limit=FETCH_LIMIT)
            processed_submissions = [process_submission(submission) for submission in submissions]
            all_submissions.extend(processed_submissions)
            logging.info(f"Fetched {len(processed_submissions)} submissions from r/{subreddit_name}")
        except PrawcoreException as e:
            logging.error(f"Error fetching data from r/{subreddit_name}: {e}")
    
    return all_submissions

def stream_to_kafka():
    """
    Main function to stream Reddit data to Kafka
    Returns True if successful, False otherwise
    """
    try:
        reddit = create_reddit_client()
        producer = create_kafka_producer()
        
        submissions = get_reddit_data(reddit)
        
        if submissions:
            logging.info(f"Fetched {len(submissions)} submissions from Reddit")
            
            for submission in submissions:
                # Send to Kafka
                producer.send(
                    KAFKA_TOPIC,
                    json.dumps(submission).encode('utf-8')
                )
            
            producer.flush()
            logging.info("Successfully sent data to Kafka")
            producer.close()  # Explicitly close the producer
            return True
        
        return False
        
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
        return False

if __name__ == "__main__":
    stream_to_kafka()