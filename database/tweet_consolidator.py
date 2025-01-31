import pandas as pd
import sqlite3
import json
from pathlib import Path
import os
from datetime import datetime
import numpy as np
import re

class TweetDatabase:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.conn = None
        self.setup_database()
    
    def setup_database(self):
        """Create the database schema"""
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Create authors table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS authors (
            author_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            display_name TEXT,
            followers_count INTEGER,
            following_count INTEGER,
            tweet_count INTEGER,
            created_at DATETIME,
            updated_at DATETIME
        )
        """)
        
        # Create tweets table with nullable author_id
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            tweet_id TEXT PRIMARY KEY,
            author_id TEXT,  -- Now nullable
            created_at DATETIME NOT NULL,
            text TEXT NOT NULL,
            language TEXT,
            retweet_count INTEGER DEFAULT 0,
            reply_count INTEGER DEFAULT 0,
            like_count INTEGER DEFAULT 0,
            quote_count INTEGER DEFAULT 0,
            referenced_tweet_id TEXT,
            FOREIGN KEY (author_id) REFERENCES authors(author_id),
            FOREIGN KEY (referenced_tweet_id) REFERENCES tweets(tweet_id)
        )
        """)
        
        # Create tweet_tokens table for mapping tweets to tokens
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tweet_tokens (
            tweet_id TEXT,
            token TEXT NOT NULL,
            confidence REAL DEFAULT 1.0,
            PRIMARY KEY (tweet_id, token),
            FOREIGN KEY (tweet_id) REFERENCES tweets(tweet_id)
        )
        """)
        
        # Create vader_sentiment table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS vader_sentiment (
            tweet_id TEXT PRIMARY KEY,
            compound_score REAL NOT NULL,
            positive_score REAL NOT NULL,
            neutral_score REAL NOT NULL,
            negative_score REAL NOT NULL,
            processed_text TEXT NOT NULL,
            FOREIGN KEY (tweet_id) REFERENCES tweets(tweet_id)
        )
        """)
        
        # Create token_sentiment_timeseries table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS token_sentiment_timeseries (
            timestamp DATETIME NOT NULL,
            token TEXT NOT NULL,
            interval TEXT NOT NULL,  -- '1s', '1m', '5m', etc.
            sentiment_mean REAL NOT NULL,
            sentiment_std REAL NOT NULL,
            tweet_count INTEGER NOT NULL,
            positive_ratio REAL NOT NULL,
            negative_ratio REAL NOT NULL,
            neutral_ratio REAL NOT NULL,
            engagement_score REAL NOT NULL,  -- weighted score of likes, retweets, etc.
            PRIMARY KEY (timestamp, token, interval)
        )
        """)
        
        # Create indices
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweet_tokens_token ON tweet_tokens(token)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sentiment_timeseries_token ON token_sentiment_timeseries(token)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sentiment_timeseries_timestamp ON token_sentiment_timeseries(timestamp)")
        
        self.conn.commit()
    
    def _extract_tokens(self, text: str) -> list:
        """Extract token mentions from tweet text"""
        if not isinstance(text, str):
            return []
            
        # Common meme coin tokens and variations
        tokens = set()
        
        # Look for $TOKEN mentions
        dollar_tokens = re.findall(r'\$([a-zA-Z0-9_]+)', text.upper())
        tokens.update(dollar_tokens)
        
        # Common tokens to track (including variations)
        common_tokens = {
            # Solana ecosystem
            'SOL', 'SOLANA', 
            # Popular meme coins
            'BONK', 'BONKZ',
            'WIF', 'DOGWIFHAT', 'DOGHAT',
            'MYRO', 'MYROTHEDOG',
            'POPCAT', 'POP',
            'BOOK', 'BOOKMAP',
            'BOME', 'BOMEMAPPER',
            'SAMO', 'SAMOYEDCOIN',
            'GUANO', 'GUANOAPES',
            'NOPE', 'NOPETOKEN',
            'POPKING', 'POPK',
            # New additions
            'DOGE', 'DOGECOIN',
            'PEPE', 'PEPECOIN',
            'SHIB', 'SHIBAINU',
            'FLOKI',
            'WOJAK',
            'COPE',
            'DUST',
            'MEME',
            'SLERF',
            'TOAD'
        }
        
        # Look for token mentions without $
        words = re.findall(r'\b[A-Za-z0-9_]+\b', text.upper())
        tokens.update(set(words) & common_tokens)
        
        # Clean and validate tokens
        valid_tokens = set()
        for token in tokens:
            # Remove common prefixes/suffixes
            token = re.sub(r'^(THE|TOKEN|COIN)_*', '', token)
            token = re.sub(r'_*(TOKEN|COIN)$', '', token)
            
            # Must be at least 2 chars and only contain valid characters
            if len(token) >= 2 and re.match(r'^[A-Z0-9_]+$', token):
                valid_tokens.add(token)
        
        return list(valid_tokens)

    def import_profile_tweets(self, profile_path):
        """Import tweets from a profile directory or CSV file"""
        try:
            if isinstance(profile_path, (str, Path)):
                profile_path = Path(profile_path)
            
            if profile_path.is_dir():
                csv_files = list(profile_path.glob("*.csv"))
                for csv_file in csv_files:
                    print(f"Processing {csv_file}...")
                    self._import_csv_tweets(csv_file)
            else:
                self._import_csv_tweets(profile_path)
                
        except Exception as e:
            print(f"Error importing profile tweets: {str(e)}")
            raise

    def _import_csv_tweets(self, csv_file):
        """Import tweets from a CSV file"""
        try:
            df = pd.read_csv(csv_file)
            
            # Map common column names
            column_mapping = {
                'id': 'tweet_id',
                'tweet_id': 'tweet_id',
                'author_id': 'author_id',
                'user_id': 'author_id',
                'username': 'username',
                'screen_name': 'username',
                'created_at': 'created_at',
                'timestamp': 'created_at',
                'date': 'created_at',
                'text': 'text',
                'tweet_text': 'text',
                'content': 'text',
                'tweet': 'text',
                'message': 'text',
                'lang': 'language',
                'language': 'language',
                'retweet_count': 'retweet_count',
                'retweets': 'retweet_count',
                'reply_count': 'reply_count',
                'replies': 'reply_count',
                'like_count': 'like_count',
                'likes': 'like_count',
                'favorite_count': 'like_count',
                'favorites': 'like_count',
                'quote_count': 'quote_count',
                'quotes': 'quote_count',
                'token': 'token',
                'symbol': 'token',
                'coin': 'token',
                'address': 'token_address'
            }
            
            # Rename columns if they exist
            df = df.rename(columns={old: new for old, new in column_mapping.items() if old in df.columns})
            
            # If no text column but we have a token column, create text from token
            if 'text' not in df.columns and 'token' in df.columns:
                df['text'] = df['token'].apply(lambda x: f"${str(x)}" if pd.notna(x) else "")
            
            # Generate tweet_id if missing using hash of text and timestamp
            if 'tweet_id' not in df.columns:
                if 'created_at' in df.columns:
                    df['tweet_id'] = df.apply(lambda row: str(hash(f"{row['text']}_{row['created_at']}")), axis=1)
                else:
                    df['tweet_id'] = df['text'].apply(lambda x: str(hash(x)))
            
            # Convert timestamp formats
            if 'created_at' in df.columns:
                try:
                    df['created_at'] = pd.to_datetime(df['created_at'])
                except:
                    df['created_at'] = pd.Timestamp.now()
            else:
                df['created_at'] = pd.Timestamp.now()
            
            cursor = self.conn.cursor()
            
            # Process each tweet
            for _, row in df.iterrows():
                try:
                    # Extract tokens from text and token column
                    tokens = self._extract_tokens(row['text'])
                    if 'token' in df.columns and pd.notna(row.get('token')):
                        tokens.append(str(row['token']).upper())
                    tokens = list(set(tokens))  # Remove duplicates
                    
                    # Store tweet
                    cursor.execute("""
                        INSERT OR IGNORE INTO tweets (
                            tweet_id, author_id, created_at, text, language,
                            retweet_count, reply_count, like_count, quote_count
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        str(row['tweet_id']),
                        row.get('author_id'),
                        row['created_at'],
                        row['text'],
                        row.get('language'),
                        row.get('retweet_count', 0),
                        row.get('reply_count', 0),
                        row.get('like_count', 0),
                        row.get('quote_count', 0)
                    ))
                    
                    # Store tokens
                    for token in tokens:
                        if token:  # Skip empty tokens
                            cursor.execute("""
                                INSERT OR IGNORE INTO tweet_tokens (tweet_id, token)
                                VALUES (?, ?)
                            """, (str(row['tweet_id']), token))
                    
                except Exception as e:
                    print(f"Error processing tweet {row.get('tweet_id')}: {str(e)}")
                    continue
            
            self.conn.commit()
            
        except Exception as e:
            print(f"Error importing CSV file {csv_file}: {str(e)}")
            raise
    
    def import_json_tweets(self, json_file):
        """Import tweets from JSON file"""
        with open(json_file, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Error reading JSON file {json_file}: {str(e)}")
                return
        
        # Handle different JSON formats
        tweets = []
        if isinstance(data, list):
            tweets = data
        elif isinstance(data, dict):
            # Handle nested structures
            for key in ['tweets', 'data', 'results']:
                if key in data:
                    tweets = data[key]
                    break
            if not tweets:
                tweets = [data]  # Single tweet object
        
        for tweet in tweets:
            try:
                # Handle string tweets (raw text)
                if isinstance(tweet, str):
                    tweet_data = {
                        'tweet_id': str(hash(tweet)),
                        'author_id': None,
                        'created_at': pd.Timestamp.now(),
                        'text': tweet,
                        'language': None,
                        'retweet_count': 0,
                        'reply_count': 0,
                        'like_count': 0,
                        'quote_count': 0
                    }
                else:
                    # Extract user/author information if available
                    user_data = tweet.get('user', tweet.get('author', {}))
                    if user_data:
                        author_data = {
                            'author_id': str(user_data.get('id_str', user_data.get('id', hash(str(user_data.get('screen_name', '')))))),
                            'username': str(user_data.get('screen_name', user_data.get('username', ''))),
                            'display_name': str(user_data.get('name', '')),
                            'followers_count': int(user_data.get('followers_count', 0)),
                            'following_count': int(user_data.get('friends_count', user_data.get('following_count', 0))),
                            'tweet_count': int(user_data.get('statuses_count', user_data.get('tweet_count', 0))),
                            'created_at': user_data.get('created_at', None)
                        }
                        self._insert_author(author_data)
                    
                    # Extract tweet data
                    tweet_data = {
                        'tweet_id': str(tweet.get('id_str', tweet.get('id', hash(str(tweet.get('text', '')))))),
                        'author_id': str(user_data.get('id_str', user_data.get('id'))) if user_data else None,
                        'created_at': tweet.get('created_at', pd.Timestamp.now()),
                        'text': str(tweet.get('full_text', tweet.get('text', ''))),
                        'language': tweet.get('lang'),
                        'retweet_count': int(tweet.get('retweet_count', 0)),
                        'reply_count': int(tweet.get('reply_count', 0)),
                        'like_count': int(tweet.get('favorite_count', tweet.get('like_count', 0))),
                        'quote_count': int(tweet.get('quote_count', 0))
                    }
                
                self._insert_tweet(tweet_data)
                
                # Extract tokens if available
                tokens = []
                if 'tokens' in tweet:
                    tokens = tweet['tokens'] if isinstance(tweet['tokens'], list) else [tweet['tokens']]
                elif 'entities' in tweet and 'hashtags' in tweet['entities']:
                    tokens = [tag['text'] for tag in tweet['entities']['hashtags']]
                
                for token in tokens:
                    self.associate_tweet_with_token(tweet_data['tweet_id'], str(token))
                    
            except Exception as e:
                print(f"Error processing tweet in {json_file}: {str(e)}")
                continue
    
    def import_hf_dataset(self, parquet_path):
        """Import tweets from Hugging Face dataset parquet file"""
        print(f"Importing Hugging Face dataset from {parquet_path}")
        
        try:
            df = pd.read_parquet(parquet_path)
            
            # Process each tweet
            for _, row in df.iterrows():
                # Extract author information if available
                if 'author' in row:
                    author_data = {
                        'author_id': row['author'].get('id', str(hash(row['author'].get('username', '')))),
                        'username': row['author'].get('username', ''),
                        'display_name': row['author'].get('name', ''),
                        'followers_count': row['author'].get('public_metrics', {}).get('followers_count', 0),
                        'following_count': row['author'].get('public_metrics', {}).get('following_count', 0),
                        'tweet_count': row['author'].get('public_metrics', {}).get('tweet_count', 0),
                        'created_at': row['author'].get('created_at', None)
                    }
                    self._insert_author(author_data)
                
                # Extract tweet data
                tweet_data = {
                    'tweet_id': row.get('id', str(hash(row.get('text', '')))),
                    'author_id': row.get('author_id', row['author'].get('id') if 'author' in row else None),
                    'created_at': row.get('created_at', pd.Timestamp.now()),
                    'text': row.get('text', ''),
                    'language': row.get('lang', None),
                    'retweet_count': row.get('public_metrics', {}).get('retweet_count', 0),
                    'reply_count': row.get('public_metrics', {}).get('reply_count', 0),
                    'like_count': row.get('public_metrics', {}).get('like_count', 0),
                    'quote_count': row.get('public_metrics', {}).get('quote_count', 0),
                    'referenced_tweet_id': None  # Add if available in the dataset
                }
                self._insert_tweet(tweet_data)
                
                # Extract token information if available
                if 'tokens' in row:
                    tokens = row['tokens'] if isinstance(row['tokens'], list) else [row['tokens']]
                    for token in tokens:
                        self.associate_tweet_with_token(tweet_data['tweet_id'], token)
                
                # If the dataset includes sentiment scores, store them
                if any(col.startswith('sentiment_') for col in row.index):
                    sentiment_scores = {
                        'compound': row.get('sentiment_compound', 0.0),
                        'positive': row.get('sentiment_positive', 0.0),
                        'negative': row.get('sentiment_negative', 0.0),
                        'neutral': row.get('sentiment_neutral', 0.0),
                        'processed_text': row.get('processed_text', row.get('text', ''))
                    }
                    self.store_vader_sentiment(tweet_data['tweet_id'], sentiment_scores)
            
            print(f"Successfully imported {len(df)} tweets from Hugging Face dataset")
            
        except Exception as e:
            print(f"Error importing Hugging Face dataset: {str(e)}")
            raise
    
    def _insert_author(self, author_data):
        """Insert author into database"""
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR REPLACE INTO authors 
        (author_id, username, display_name, followers_count, following_count, tweet_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            author_data['author_id'],
            author_data['username'],
            author_data['display_name'],
            author_data['followers_count'],
            author_data['following_count'],
            author_data['tweet_count'],
            author_data['created_at']
        ))
        self.conn.commit()
    
    def _insert_tweet(self, tweet_data):
        """Insert tweet into database"""
        cursor = self.conn.cursor()
        
        # Convert timestamp to string format
        created_at = tweet_data['created_at']
        if isinstance(created_at, pd.Timestamp):
            created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute("""
        INSERT OR REPLACE INTO tweets 
        (tweet_id, author_id, created_at, text, language, retweet_count, reply_count, like_count, quote_count, referenced_tweet_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tweet_data['tweet_id'],
            tweet_data['author_id'],
            created_at,
            tweet_data['text'],
            tweet_data['language'],
            tweet_data['retweet_count'],
            tweet_data['reply_count'],
            tweet_data['like_count'],
            tweet_data['quote_count'],
            tweet_data.get('referenced_tweet_id')
        ))
        self.conn.commit()
    
    def associate_tweet_with_token(self, tweet_id, token, confidence=1.0):
        """Associate a tweet with a token"""
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR REPLACE INTO tweet_tokens (tweet_id, token, confidence)
        VALUES (?, ?, ?)
        """, (tweet_id, token, confidence))
        self.conn.commit()
    
    def store_vader_sentiment(self, tweet_id, sentiment_scores):
        """Store VADER sentiment scores for a tweet"""
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR REPLACE INTO vader_sentiment 
        (tweet_id, compound_score, positive_score, neutral_score, negative_score, processed_text)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            tweet_id,
            sentiment_scores['compound'],
            sentiment_scores['positive'],
            sentiment_scores['neutral'],
            sentiment_scores['negative'],
            sentiment_scores['processed_text']
        ))
        self.conn.commit()
    
    def update_token_sentiment_timeseries(self, token, interval='1m'):
        """Update sentiment timeseries for a token"""
        cursor = self.conn.cursor()
        
        # Get tweets and their sentiment scores for the token
        cursor.execute("""
        SELECT 
            t.created_at,
            vs.compound_score,
            vs.positive_score,
            vs.negative_score,
            vs.neutral_score,
            t.retweet_count,
            t.like_count,
            t.reply_count,
            t.quote_count
        FROM tweets t
        JOIN tweet_tokens tt ON t.tweet_id = tt.tweet_id
        JOIN vader_sentiment vs ON t.tweet_id = vs.tweet_id
        WHERE tt.token = ?
        ORDER BY t.created_at
        """, (token,))
        
        results = cursor.fetchall()
        if not results:
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(results, columns=[
            'created_at', 'compound_score', 'positive_score', 'negative_score',
            'neutral_score', 'retweet_count', 'like_count', 'reply_count', 'quote_count'
        ])
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Resample by interval
        grouped = df.resample(interval, on='created_at')
        
        # Calculate metrics
        metrics = grouped.agg({
            'compound_score': ['mean', 'std', 'count'],
            'positive_score': 'mean',
            'negative_score': 'mean',
            'neutral_score': 'mean',
            'retweet_count': 'sum',
            'like_count': 'sum',
            'reply_count': 'sum',
            'quote_count': 'sum'
        })
        
        # Calculate engagement score
        metrics['engagement_score'] = (
            metrics['retweet_count'] * 2 +
            metrics['like_count'] +
            metrics['reply_count'] * 1.5 +
            metrics['quote_count'] * 1.5
        )
        
        # Store results
        for timestamp, row in metrics.iterrows():
            cursor.execute("""
            INSERT OR REPLACE INTO token_sentiment_timeseries
            (timestamp, token, interval, sentiment_mean, sentiment_std, tweet_count,
             positive_ratio, negative_ratio, neutral_ratio, engagement_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp,
                token,
                interval,
                row['compound_score']['mean'],
                row['compound_score']['std'],
                row['compound_score']['count'],
                row['positive_score']['mean'],
                row['negative_score']['mean'],
                row['neutral_score']['mean'],
                row['engagement_score']
            ))
        
        self.conn.commit()

def consolidate_tweets():
    """Consolidate all tweet data into the database"""
    base_dir = Path(__file__).parent.parent
    db = TweetDatabase(base_dir / 'sentiment_data.db')
    
    # Process root directory CSV files
    root_csv_files = list(base_dir.glob("*.csv"))
    for csv_file in root_csv_files:
        print(f"Processing root CSV file: {csv_file.name}")
        try:
            db.import_profile_tweets(csv_file)
        except Exception as e:
            print(f"Error processing {csv_file.name}: {str(e)}")
            continue
    
    # Process profile data
    profile_dir = base_dir / 'profile_data'
    if profile_dir.exists():
        for profile in profile_dir.iterdir():
            if profile.is_dir():
                print(f"Processing profile: {profile.name}")
                db.import_profile_tweets(profile)
    
    # Process twitter_data folder
    twitter_data_dir = base_dir / 'twitter_data'
    if twitter_data_dir.exists():
        csv_files = list(twitter_data_dir.glob("*.csv"))
        for csv_file in sorted(csv_files):
            print(f"Processing Twitter data file: {csv_file.name}")
            try:
                df = pd.read_csv(csv_file)
                # Check if this is a sentiment data file
                if 'sentiment' in csv_file.name.lower():
                    # These files might already have sentiment scores
                    if any(col.startswith('sentiment_') for col in df.columns):
                        for _, row in df.iterrows():
                            sentiment_scores = {
                                'compound': row.get('sentiment_compound', 0.0),
                                'positive': row.get('sentiment_positive', 0.0),
                                'negative': row.get('sentiment_negative', 0.0),
                                'neutral': row.get('sentiment_neutral', 0.0),
                                'processed_text': row.get('processed_text', row.get('text', ''))
                            }
                            db.store_vader_sentiment(row['tweet_id'], sentiment_scores)
                
                # Import as regular tweet data
                db.import_profile_tweets(csv_file)
            except Exception as e:
                print(f"Error processing {csv_file.name}: {str(e)}")
                continue
    
    # Process JSON files
    json_files = list(base_dir.rglob("*tweet*.json"))
    for json_file in json_files:
        print(f"Processing JSON file: {json_file}")
        db.import_json_tweets(json_file)
    
    # Import Hugging Face datasets
    hf_datasets = [
        "hf://datasets/MasaFoundation/bankless_ROLLUP_Memecoin_Mania__Solana_ATH__Blackrock_Ethereum_Fund/data/train-00000-of-00001.parquet",
        "hf://datasets/MasaFoundation/memecoin_all_tweets_2024-08-08_10-48-28/data/train-00000-of-00001.parquet"
    ]
    
    for dataset_path in hf_datasets:
        try:
            print(f"Importing Hugging Face dataset from {dataset_path}")
            db.import_hf_dataset(dataset_path)
        except Exception as e:
            print(f"Warning: Could not import Hugging Face dataset {dataset_path}: {str(e)}")
    
    print("Tweet consolidation complete!")
    return db

if __name__ == "__main__":
    consolidate_tweets()
