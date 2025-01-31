from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import sqlite3
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MemeCoinVaderAnalyzer:
    def __init__(self, db_path: str = 'sentiment_data.db'):
        """Initialize the analyzer with custom lexicon"""
        self.analyzer = SentimentIntensityAnalyzer()
        self.db_path = db_path
        
        # Add custom lexicon for crypto/meme terms
        self.analyzer.lexicon.update({
            # Bullish terms
            'moon': 4.0,
            'mooning': 4.0,
            'moonshot': 4.0,
            'bullish': 3.0,
            'hodl': 2.0,
            'pump': 2.0,
            'pumping': 2.5,
            'ath': 3.0,
            'launch': 2.0,
            'launching': 2.0,
            'gem': 2.5,
            'lambo': 3.0,
            'fomo': 1.5,
            
            # Bearish terms
            'dump': -2.0,
            'dumping': -2.5,
            'rug': -4.0,
            'rugpull': -4.0,
            'scam': -3.0,
            'ponzi': -3.5,
            'bearish': -3.0,
            'crash': -3.0,
            'crashing': -3.5,
            'rekt': -3.0,
            
            # Meme-specific
            'wagmi': 2.0,
            'ngmi': -2.0,
            'lfg': 3.0,
            'gm': 1.0,
            'degen': 0.5,
            'ser': 0.5,
            'fren': 1.0,
            'anon': 0.0,
            'wen': 0.0,
            
            # Solana specific
            'solana': 1.0,
            'sol': 1.0,
            'bonk': 1.0,
            'myro': 1.0,
            'dogwifhat': 1.0,
            'wif': 1.0,
        })

    def _clean_text(self, text: str) -> str:
        """Clean tweet text for sentiment analysis"""
        if not isinstance(text, str):
            return ""
            
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Remove user mentions
        text = re.sub(r'@\w+', '', text)
        
        # Remove hashtag symbol but keep the text
        text = re.sub(r'#(\w+)', r'\1', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text

    def analyze_sentiment(self, text: str) -> dict:
        """Analyze sentiment of a single piece of text"""
        clean_text = self._clean_text(text)
        scores = self.analyzer.polarity_scores(clean_text)
        scores['processed_text'] = clean_text
        return scores

    def process_tweets(self, batch_size: int = 1000):
        """Process all unanalyzed tweets in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get tweets that haven't been analyzed yet
            cursor.execute("""
                SELECT t.tweet_id, t.text
                FROM tweets t
                LEFT JOIN vader_sentiment v ON t.tweet_id = v.tweet_id
                WHERE v.tweet_id IS NULL
            """)
            
            total_processed = 0
            while True:
                tweets = cursor.fetchmany(batch_size)
                if not tweets:
                    break
                
                for tweet_id, text in tweets:
                    sentiment = self.analyze_sentiment(text)
                    
                    # Store sentiment scores
                    cursor.execute("""
                        INSERT INTO vader_sentiment 
                        (tweet_id, compound_score, positive_score, neutral_score, 
                         negative_score, processed_text)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        tweet_id,
                        sentiment['compound'],
                        sentiment['pos'],
                        sentiment['neu'],
                        sentiment['neg'],
                        sentiment['processed_text']
                    ))
                    
                    total_processed += 1
                    if total_processed % 1000 == 0:
                        logger.info(f"Processed {total_processed} tweets")
                
                conn.commit()
            
            logger.info(f"Completed sentiment analysis for {total_processed} tweets")
            
        except Exception as e:
            logger.error(f"Error processing tweets: {str(e)}")
            conn.rollback()
            raise
        
        finally:
            conn.close()

    def update_timeseries(self, interval: str = '1h'):
        """Update sentiment timeseries data for all tokens"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get all unique tokens
            cursor.execute("SELECT DISTINCT token FROM tweet_tokens")
            tokens = [row[0] for row in cursor.fetchall()]
            
            for token in tokens:
                # Calculate sentiment metrics for each time interval
                cursor.execute("""
                    WITH tweet_sentiments AS (
                        SELECT 
                            t.created_at,
                            v.compound_score,
                            CASE WHEN v.compound_score >= 0.05 THEN 1 ELSE 0 END as is_positive,
                            CASE WHEN v.compound_score <= -0.05 THEN 1 ELSE 0 END as is_negative,
                            CASE WHEN v.compound_score > -0.05 AND v.compound_score < 0.05 THEN 1 ELSE 0 END as is_neutral,
                            (t.like_count + t.retweet_count * 2 + t.reply_count + t.quote_count) as engagement
                        FROM tweets t
                        JOIN tweet_tokens tk ON t.tweet_id = tk.tweet_id
                        JOIN vader_sentiment v ON t.tweet_id = v.tweet_id
                        WHERE tk.token = ?
                    )
                    SELECT 
                        strftime(?, created_at) as interval_timestamp,
                        AVG(compound_score) as sentiment_mean,
                        SQRT(AVG(compound_score * compound_score) - AVG(compound_score) * AVG(compound_score)) as sentiment_std,
                        COUNT(*) as tweet_count,
                        AVG(CAST(is_positive as FLOAT)) as positive_ratio,
                        AVG(CAST(is_negative as FLOAT)) as negative_ratio,
                        AVG(CAST(is_neutral as FLOAT)) as neutral_ratio,
                        AVG(engagement) as engagement_score
                    FROM tweet_sentiments
                    GROUP BY interval_timestamp
                """, (token, interval))
                
                results = cursor.fetchall()
                
                # Store results
                for row in results:
                    cursor.execute("""
                        INSERT OR REPLACE INTO token_sentiment_timeseries
                        (timestamp, token, interval, sentiment_mean, sentiment_std,
                         tweet_count, positive_ratio, negative_ratio, neutral_ratio,
                         engagement_score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (row[0], token, interval, row[1], row[2], row[3],
                         row[4], row[5], row[6], row[7]))
            
            conn.commit()
            logger.info(f"Updated sentiment timeseries for {len(tokens)} tokens")
            
        except Exception as e:
            logger.error(f"Error updating timeseries: {str(e)}")
            conn.rollback()
            raise
            
        finally:
            conn.close()

if __name__ == "__main__":
    # Initialize analyzer with the database
    base_dir = Path(__file__).parent.parent
    analyzer = MemeCoinVaderAnalyzer(base_dir / 'sentiment_data.db')
    
    # Process all unanalyzed tweets
    logger.info("Starting sentiment analysis...")
    analyzer.process_tweets()
    
    # Update timeseries for different intervals
    logger.info("Updating sentiment timeseries...")
    for interval in ['%Y-%m-%d %H:00:00', '%Y-%m-%d', '%Y-%m']:  # hourly, daily, monthly
        analyzer.update_timeseries(interval)
    
    logger.info("Sentiment analysis complete!")
