import pandas as pd
import sqlite3
from pathlib import Path
from vaderSentiment.vaderSentiment import SentimentIntensifier, SentimentAnalyzer
import re
from datetime import datetime
import numpy as np

class MemeSentimentAnalyzer:
    def __init__(self):
        # Initialize VADER with custom configurations
        self.analyzer = SentimentAnalyzer()
        
        # Add crypto/meme-specific lexicon
        self.custom_lexicon = {
            # Positive meme terms
            'moon': 4.0,
            'hodl': 2.0,
            'diamond hands': 3.0,
            'bullish': 3.0,
            'lfg': 3.0,
            'gm': 1.0,
            'wagmi': 2.0,
            'fomo': 1.5,
            
            # Negative meme terms
            'rug': -4.0,
            'rugpull': -4.0,
            'dump': -3.0,
            'scam': -3.5,
            'ngmi': -2.0,
            'paper hands': -2.0,
            'rekt': -3.0,
            
            # Intensity modifiers
            'ser': 1.2,
            'very': 1.3,
            'huge': 1.4,
            'massive': 1.4,
        }
        
        # Update VADER lexicon with custom terms
        self.analyzer.lexicon.update(self.custom_lexicon)
    
    def _preprocess_tweet(self, text):
        """Preprocess tweet text for better sentiment analysis"""
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Remove user mentions
        text = re.sub(r'@\w+', '', text)
        
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Convert common crypto slang
        text = text.replace('gm', 'good morning')
        text = text.replace('ngmi', 'not going to make it')
        text = text.replace('wagmi', 'we are going to make it')
        text = text.replace('lfg', 'lets go')
        
        return text
    
    def analyze_tweet(self, tweet_text):
        """Analyze sentiment of a single tweet"""
        processed_text = self._preprocess_tweet(tweet_text)
        scores = self.analyzer.polarity_scores(processed_text)
        
        return {
            'compound': scores['compound'],
            'pos': scores['pos'],
            'neu': scores['neu'],
            'neg': scores['neg'],
            'processed_text': processed_text
        }
    
    def analyze_tweets_batch(self, tweets_df):
        """Analyze sentiment for a batch of tweets"""
        results = []
        
        for _, tweet in tweets_df.iterrows():
            sentiment = self.analyze_tweet(tweet['text'])
            results.append({
                'timestamp': tweet['timestamp'],
                'text': tweet['text'],
                'processed_text': sentiment['processed_text'],
                'compound_score': sentiment['compound'],
                'positive_score': sentiment['pos'],
                'neutral_score': sentiment['neu'],
                'negative_score': sentiment['neg'],
                'token': tweet['token'] if 'token' in tweet.index else None
            })
        
        return pd.DataFrame(results)
    
    def calculate_token_sentiment(self, sentiment_df, time_window='1S'):
        """Calculate aggregated sentiment metrics for each token over time
        
        Args:
            sentiment_df: DataFrame with sentiment analysis results
            time_window: Time window for aggregation. Default '1S' for 1 second.
                        Use pandas frequency strings: 'S' for seconds, 'T' or 'min' for minutes
        """
        # Group by token and time window
        grouped = sentiment_df.groupby(['token', pd.Grouper(key='timestamp', freq=time_window)])
        
        # Calculate metrics
        sentiment_metrics = grouped.agg({
            'compound_score': ['mean', 'std', 'count'],
            'positive_score': 'mean',
            'negative_score': 'mean',
            'neutral_score': 'mean'
        }).reset_index()
        
        # Flatten column names
        sentiment_metrics.columns = [
            'token', 'timestamp', 'sentiment_mean', 'sentiment_std', 'tweet_count',
            'positive_ratio', 'negative_ratio', 'neutral_ratio'
        ]
        
        return sentiment_metrics

def create_sentiment_database():
    """Create SQLite database for storing sentiment data"""
    base_dir = Path(__file__).parent.parent
    db_path = base_dir / 'sentiment_data.db'
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tweets table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tweets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        token TEXT NOT NULL,
        text TEXT NOT NULL,
        processed_text TEXT NOT NULL,
        compound_score REAL NOT NULL,
        positive_score REAL NOT NULL,
        neutral_score REAL NOT NULL,
        negative_score REAL NOT NULL,
        UNIQUE(timestamp, token, text)
    )
    """)
    
    # Create sentiment metrics table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS token_sentiment (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        token TEXT NOT NULL,
        sentiment_mean REAL NOT NULL,
        sentiment_std REAL NOT NULL,
        tweet_count INTEGER NOT NULL,
        positive_ratio REAL NOT NULL,
        negative_ratio REAL NOT NULL,
        neutral_ratio REAL NOT NULL,
        UNIQUE(timestamp, token)
    )
    """)
    
    # Create indices
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_token ON tweets(token)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_timestamp ON tweets(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sentiment_token ON token_sentiment(token)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sentiment_timestamp ON token_sentiment(timestamp)")
    
    conn.commit()
    conn.close()
    
    print(f"Created sentiment database at {db_path}")
    return db_path

def process_twitter_data(twitter_data_path):
    """Process Twitter data and store sentiment analysis results"""
    # Initialize analyzer
    analyzer = MemeSentimentAnalyzer()
    
    # Create database
    db_path = create_sentiment_database()
    
    # Load Twitter data
    tweets_df = pd.read_csv(twitter_data_path)
    tweets_df['timestamp'] = pd.to_datetime(tweets_df['timestamp'])
    
    # Analyze sentiments
    print("Analyzing tweet sentiments...")
    sentiment_results = analyzer.analyze_tweets_batch(tweets_df)
    
    # Calculate token-level metrics
    print("Calculating token-level sentiment metrics...")
    token_metrics = analyzer.calculate_token_sentiment(sentiment_results)
    
    # Store results in database
    print("Storing results in database...")
    conn = sqlite3.connect(db_path)
    
    sentiment_results.to_sql('tweets', conn, if_exists='append', index=False,
                           method='multi', chunksize=1000)
    token_metrics.to_sql('token_sentiment', conn, if_exists='append', index=False,
                        method='multi', chunksize=1000)
    
    conn.close()
    print("Done! Sentiment analysis results have been stored in the database.")

if __name__ == "__main__":
    # Example usage
    twitter_data_path = Path(__file__).parent.parent / 'twitter_data.csv'
    if twitter_data_path.exists():
        process_twitter_data(twitter_data_path)
    else:
        print(f"Please place your Twitter data at {twitter_data_path}")
