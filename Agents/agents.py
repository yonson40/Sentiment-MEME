from typing import Annotated, TypeVar
from datetime import datetime, timedelta
import sqlite3
import os
from pathlib import Path

from langchain_core.agents import AgentAction, AgentFinish
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph

from .schema import AgentState, OHLCVData, Tweet, SentimentScore, TokenSentiment
from twitter.create_tweet_database import TweetDatabaseCreator
from twitter.clean_ohlcv_data import clean_and_standardize_ohlcv_data

# Type for state
State = TypeVar("State", bound=AgentState)

class OHLCVAgent(BaseTool):
    name = "ohlcv_agent"
    description = "Agent responsible for collecting and updating OHLCV data"
    
    def __init__(self):
        super().__init__()
        self.project_root = str(Path(__file__).parent.parent)
        self.db_path = os.path.join(self.project_root, 'sentiment.db')

    def _run(self, state: State) -> State:
        try:
            # Clean and standardize new OHLCV data
            clean_and_standardize_ohlcv_data()
            
            # Connect to database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Process each file in the standardized directory
            ohlcv_dir = os.path.join(self.project_root, 'ohlcv_data_standardized')
            
            for filename in os.listdir(ohlcv_dir):
                if filename.endswith('.csv'):
                    token = filename.replace('_ohlcv.csv', '')
                    filepath = os.path.join(ohlcv_dir, filename)
                    
                    with open(filepath, 'r') as f:
                        next(f)  # Skip header
                        for line in f:
                            datetime_str, open_price, high, low, close, volume = line.strip().split(',')
                            
                            # Check if record exists
                            cursor.execute('''
                                SELECT 1 FROM ohlcv_data 
                                WHERE token = ? AND datetime = ?
                            ''', (token, datetime_str))
                            
                            if not cursor.fetchone():
                                data = OHLCVData(
                                    token=token,
                                    datetime=datetime_str,
                                    open=float(open_price),
                                    high=float(high),
                                    low=float(low),
                                    close=float(close),
                                    volume=float(volume)
                                )
                                state.ohlcv_updates.append(data)
                                
                                cursor.execute('''
                                    INSERT INTO ohlcv_data (token, datetime, open, high, low, close, volume)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                ''', (token, datetime_str, open_price, high, low, close, volume))
            
            conn.commit()
            conn.close()
            
            state.status = "ohlcv_updated"
            state.last_run = datetime.now()
            
        except Exception as e:
            state.error_messages.append(f"OHLCV Error: {str(e)}")
            state.status = "error"
            
        return state

class TweetAgent(BaseTool):
    name = "tweet_agent"
    description = "Agent responsible for collecting and processing tweets"
    
    def __init__(self):
        super().__init__()
        self.project_root = str(Path(__file__).parent.parent)
        self.db_path = os.path.join(self.project_root, 'sentiment.db')
        self.tweet_creator = TweetDatabaseCreator(self.db_path)

    def _run(self, state: State) -> State:
        try:
            self.tweet_creator.connect()
            
            # Get tweets from the last 24 hours
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=1)
            
            # Collect new tweets
            new_tweets = self.tweet_creator.collect_tweets(start_time, end_time)
            state.new_tweets.extend(new_tweets)
            
            state.status = "tweets_collected"
            state.last_run = datetime.now()
            
        except Exception as e:
            state.error_messages.append(f"Tweet Collection Error: {str(e)}")
            state.status = "error"
        finally:
            if hasattr(self.tweet_creator, 'conn') and self.tweet_creator.conn:
                self.tweet_creator.conn.close()
            
        return state

class SentimentAgent(BaseTool):
    name = "sentiment_agent"
    description = "Agent responsible for sentiment analysis and timeseries updates"
    
    def __init__(self):
        super().__init__()
        self.project_root = str(Path(__file__).parent.parent)
        self.db_path = os.path.join(self.project_root, 'sentiment.db')
        self.tweet_creator = TweetDatabaseCreator(self.db_path)

    def _run(self, state: State) -> State:
        try:
            self.tweet_creator.connect()
            
            # Calculate sentiment for new tweets
            for tweet in state.new_tweets:
                sentiment = self.tweet_creator.calculate_sentiment_for_tweet(tweet)
                state.sentiment_scores[tweet.tweet_id] = sentiment
            
            # Update token sentiment timeseries
            token_sentiments = self.tweet_creator.update_token_sentiment_timeseries()
            state.token_sentiments.extend(token_sentiments)
            
            state.status = "sentiment_updated"
            state.last_run = datetime.now()
            
        except Exception as e:
            state.error_messages.append(f"Sentiment Analysis Error: {str(e)}")
            state.status = "error"
        finally:
            if hasattr(self.tweet_creator, 'conn') and self.tweet_creator.conn:
                self.tweet_creator.conn.close()
            
        return state

def should_continue(state: State) -> str:
    """Determine if we should continue processing or end"""
    if state.status == "error":
        return END
    elif state.status == "sentiment_updated":
        return END
    elif state.status == "ohlcv_updated":
        return "tweet_agent"
    elif state.status == "tweets_collected":
        return "sentiment_agent"
    else:
        return "ohlcv_agent"
