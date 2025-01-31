import os
import pandas as pd
import sqlite3
from datetime import datetime
import logging
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TweetDatabaseCreator:
    def __init__(self, db_path='sentiment_data.db'):
        """Initialize the database creator"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def connect(self):
        """Create database connection"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    def create_tables(self):
        """Create the necessary tables"""
        try:
            # Create tweets table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tweets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT UNIQUE,
                    username TEXT,
                    timestamp DATETIME,
                    text TEXT,
                    likes INTEGER DEFAULT 0,
                    retweets INTEGER DEFAULT 0,
                    replies INTEGER DEFAULT 0,
                    has_image BOOLEAN DEFAULT FALSE,
                    has_video BOOLEAN DEFAULT FALSE,
                    is_retweet BOOLEAN DEFAULT FALSE,
                    is_reply BOOLEAN DEFAULT FALSE,
                    is_meme BOOLEAN DEFAULT FALSE,
                    meme_relevance_score FLOAT,
                    sentiment_score FLOAT,
                    sentiment_details TEXT,
                    source_file TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create tokens table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_symbol TEXT UNIQUE,
                    token_address TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create tweet_tokens table (many-to-many relationship)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tweet_tokens (
                    tweet_id INTEGER,
                    token_id INTEGER,
                    PRIMARY KEY (tweet_id, token_id),
                    FOREIGN KEY (tweet_id) REFERENCES tweets(id),
                    FOREIGN KEY (token_id) REFERENCES tokens(id)
                )
            """)

            # Create indexes
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_timestamp ON tweets(timestamp)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_username ON tweets(username)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_is_meme ON tweets(is_meme)")
            
            self.conn.commit()
            logger.info("Tables and indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise

    def process_profile_tweets(self):
        """Process tweets from profile_data directory"""
        profile_dir = 'profile_data'
        if not os.path.exists(profile_dir):
            logger.warning(f"Directory not found: {profile_dir}")
            return

        for username in os.listdir(profile_dir):
            tweets_file = os.path.join(profile_dir, username, 'tweets.csv')
            if os.path.exists(tweets_file):
                try:
                    df = pd.read_csv(tweets_file)
                    self.insert_tweets(df, tweets_file)
                    logger.info(f"Processed tweets from {tweets_file}")
                except Exception as e:
                    logger.error(f"Error processing {tweets_file}: {str(e)}")

    def process_sentiment_data(self):
        """Process tweets from sentiment_data directory"""
        sentiment_dir = 'sentiment_data'
        if not os.path.exists(sentiment_dir):
            logger.warning(f"Directory not found: {sentiment_dir}")
            return

        for file in os.listdir(sentiment_dir):
            if file.endswith('.csv'):
                file_path = os.path.join(sentiment_dir, file)
                try:
                    df = pd.read_csv(file_path)
                    self.insert_tweets(df, file_path)
                    logger.info(f"Processed tweets from {file_path}")
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")

    def process_twitter_data(self):
        """Process tweets from twitter_data directory"""
        twitter_dir = 'twitter_data'
        if not os.path.exists(twitter_dir):
            logger.warning(f"Directory not found: {twitter_dir}")
            return

        for file in os.listdir(twitter_dir):
            if file.endswith('.csv'):
                file_path = os.path.join(twitter_dir, file)
                try:
                    df = pd.read_csv(file_path)
                    self.insert_tweets(df, file_path)
                    logger.info(f"Processed tweets from {file_path}")
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")

    def insert_tweets(self, df, source_file):
        """Insert tweets into database"""
        try:
            # Standardize column names
            df.columns = df.columns.str.lower()
            
            # Map DataFrame columns to database columns
            tweet_data = []
            token_data = set()
            
            for _, row in df.iterrows():
                # Extract tweet data
                tweet = {
                    'tweet_id': str(row.get('tweet_id', '')),
                    'username': row.get('username', ''),
                    'timestamp': row.get('timestamp', ''),
                    'text': row.get('text', ''),
                    'likes': int(row.get('likes', 0)),
                    'retweets': int(row.get('retweets', 0)),
                    'replies': int(row.get('replies', 0)),
                    'has_image': bool(row.get('has_image', False)),
                    'has_video': bool(row.get('has_video', False)),
                    'is_retweet': bool(row.get('is_retweet', False)),
                    'is_reply': bool(row.get('is_reply', False)),
                    'is_meme': bool(row.get('is_meme', False)),
                    'meme_relevance_score': float(row.get('meme_relevance_score', 0)),
                    'sentiment_score': float(row.get('sentiment', 0)),
                    'sentiment_details': row.get('sentiment_details', ''),
                    'source_file': source_file
                }
                tweet_data.append(tweet)
                
                # Extract token data if present
                if 'token' in row and 'address' in row:
                    token_data.add((row['token'], row['address']))

            # Insert tweets
            self.cursor.executemany("""
                INSERT OR IGNORE INTO tweets (
                    tweet_id, username, timestamp, text, likes, retweets, replies,
                    has_image, has_video, is_retweet, is_reply, is_meme,
                    meme_relevance_score, sentiment_score, sentiment_details, source_file
                ) VALUES (
                    :tweet_id, :username, :timestamp, :text, :likes, :retweets, :replies,
                    :has_image, :has_video, :is_retweet, :is_reply, :is_meme,
                    :meme_relevance_score, :sentiment_score, :sentiment_details, :source_file
                )
            """, tweet_data)

            # Insert tokens
            for token, address in token_data:
                self.cursor.execute("""
                    INSERT OR IGNORE INTO tokens (token_symbol, token_address)
                    VALUES (?, ?)
                """, (token, address))

            self.conn.commit()
            logger.info(f"Inserted {len(tweet_data)} tweets from {source_file}")
            
        except Exception as e:
            logger.error(f"Error inserting tweets from {source_file}: {str(e)}")
            self.conn.rollback()

    def create_database(self):
        """Create and populate the database"""
        try:
            self.connect()
            self.create_tables()
            
            # Process all data sources
            self.process_profile_tweets()
            self.process_sentiment_data()
            self.process_twitter_data()
            
            # Print summary
            self.cursor.execute("SELECT COUNT(*) FROM tweets")
            total_tweets = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM tweets WHERE is_meme = TRUE")
            meme_tweets = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(DISTINCT username) FROM tweets")
            unique_users = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM tokens")
            total_tokens = self.cursor.fetchone()[0]
            
            logger.info("\nDatabase Summary:")
            logger.info(f"Total Tweets: {total_tweets}")
            logger.info(f"Meme Tweets: {meme_tweets}")
            logger.info(f"Unique Users: {unique_users}")
            logger.info(f"Total Tokens: {total_tokens}")
            
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            raise
        finally:
            if self.conn:
                self.conn.close()

def main():
    creator = TweetDatabaseCreator()
    creator.create_database()

if __name__ == "__main__":
    main()
