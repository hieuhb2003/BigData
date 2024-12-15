import psycopg2
from psycopg2 import Error

def create_tables():
    # Database connection parameters
    db_params = {
        "host": "localhost",
        "database": "reddit-data",
        "user": "postgres",
        "password": "postgres"
    }

    # SQL commands to create tables
    commands = [
        """
        DROP TABLE IF EXISTS time_engagement_stats CASCADE;
        DROP TABLE IF EXISTS player_mentions_stats CASCADE;
        DROP TABLE IF EXISTS post_entities CASCADE;
        DROP TABLE IF EXISTS reddit_posts CASCADE;
        """,
        """
        CREATE TABLE reddit_posts (
            post_id VARCHAR(255) PRIMARY KEY,
            title TEXT,
            author VARCHAR(255),
            post_time TIMESTAMP,
            upvotes INTEGER,
            downvotes INTEGER,
            num_comments INTEGER,
            score INTEGER,
            text TEXT,
            first_level_comments_count INTEGER,
            second_level_comments_count INTEGER,
            subreddit VARCHAR(255),
            hour_of_day INTEGER,
            day_of_week INTEGER,
            created_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE post_entities (
            id SERIAL PRIMARY KEY,
            post_id VARCHAR(255) REFERENCES reddit_posts(post_id),
            entity_name VARCHAR(255),
            entity_type VARCHAR(50),
            mention_count INTEGER,
            created_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE player_mentions_stats (
            player_name VARCHAR(255) PRIMARY KEY,
            total_mentions INTEGER,
            total_posts INTEGER,
            total_upvotes INTEGER,
            avg_post_score FLOAT,
            highest_upvoted_post_id VARCHAR(255),
            lowest_upvoted_post_id VARCHAR(255),
            last_updated TIMESTAMP
        )
        """,
        """
        CREATE TABLE time_engagement_stats (
            hour_of_day INTEGER,
            day_of_week INTEGER,
            avg_upvotes FLOAT,
            avg_comments FLOAT,
            total_posts INTEGER,
            last_updated TIMESTAMP,
            PRIMARY KEY (hour_of_day, day_of_week)
        )
        """
    ]

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Execute each command
        for command in commands:
            print(f"Executing: {command[:50]}...")  # Print first 50 chars of each command
            cur.execute(command)

        # Commit the changes
        conn.commit()
        print("Tables created successfully!")

    except (Exception, Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")

    finally:
        # Close database connection
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    create_tables()
