from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import requests


class TwitterAgent(BaseModel):
    """
    A Pydantic based AI agent to scrape Twitter feed and the accounts you follow
    for sentiment data on memecoins. Optionally, it can compare or integrate with
    previous scraped data provided via a file path.
    """
    api_key: str
    api_secret: str
    access_token: str
    access_token_secret: str
    following_accounts: List[str]
    previous_scraped_data_path: Optional[str] = None

    def __init__(self, api_key, api_secret, access_token, access_token_secret, following_accounts, previous_scraped_data_path=None):
        self.scraper = TwitterScraper(
            username=api_key,  # Using API key as username
            password=api_secret  # Using API secret as password
        )
        self.following = following_accounts
        self.consolidator = TweetConsolidator()
        self.scrape_config = {
            'max_retries': 5,
            'request_timeout': 30,
            'max_tweets_per_account': 10000,
            'meme_keywords': ['meme', 'dank', 'viral', 'wojak', 'pepe'],
            'min_engagement': {
                'likes': 1000,
                'retweets': 50,
                'replies': 20
            }
        }

    def scrape_twitter(self) -> List[Dict[str, Any]]:
        import time
        import datetime
        import logging

        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

        # Import the Selenium-based TwitterScraper from twitter/twitter_scraper.py
        from twitter.twitter_scraper import TwitterScraper

        # Initialize the TwitterScraper and log into Twitter
        scraper = TwitterScraper()
        scraper.login_to_twitter()

        tweets = []
        now = datetime.datetime.now()

        # Scrape tweets from home timeline (your account) for the past 15 hours
        try:
            home_tweets = scraper.scrape_tweets(query="", days_back=15, limit=200)
            tweets.extend(home_tweets)
            logger.info(f"Scraped {len(home_tweets)} tweets from home timeline in the last 15 hours")
        except Exception as e:
            logger.error(f"Error scraping home timeline: {e}")

        # For each account you follow, scrape tweets from the past 24 hours
        for account in self.following_accounts:
            try:
                account_tweets = scraper.scrape_tweets(query=f"from:{account}", days_back=24, limit=200)
                tweets.extend(account_tweets)
                logger.info(f"Scraped {len(account_tweets)} tweets from account {account} in the last 24 hours")
            except Exception as e:
                logger.error(f"Error scraping tweets from account {account}: {e}")
            time.sleep(2)  # small delay to avoid rate limiting

        # Save the scraped tweets data to a CSV file
        import os
        import pandas as pd
        os.makedirs('twitter_data', exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        save_path = os.path.join('twitter_data', f'scraped_tweets_{timestamp}.csv')
        pd.DataFrame(tweets).to_csv(save_path, index=False)
        logger.info(f"Saved scraped tweets to {save_path}")

        return tweets

    def analyze_meme_potential(self, tweet_df):
        """Add meme virality predictions"""
        # Feature engineering
        tweet_df['meme_score'] = tweet_df['text'].apply(
            lambda x: sum(1 for kw in self.scrape_config['meme_keywords'] if kw in x.lower())
        )
        tweet_df['engagement_ratio'] = (
            tweet_df['likes'] + 
            tweet_df['retweets'] * 2 + 
            tweet_df['replies'] * 1.5
        ) / tweet_df['follower_count'].replace(0, 1)
        
        # Simple predictive model
        tweet_df['virality_score'] = (
            tweet_df['meme_score'] * 0.4 +
            tweet_df['engagement_ratio'] * 0.6 +
            tweet_df['sentiment_polarity'].abs() * 0.2
        )
        
        tweet_df['predicted_viral'] = tweet_df['virality_score'] > 6.0
        return tweet_df


class TokenDataFetcherAgent(BaseModel):
    """
    A Pydantic based AI agent that fetches historical OHLCV token data
    using Bitquery and DexRabbit APIs. Provide your API keys and token symbol.
    """
    bitquery_api_key: str
    dexrabbit_api_key: str
    token_symbol: str
    historical_data_source: str = Field(default="bitquery", description="Choose between 'bitquery' and 'dexrabbit'")

    def fetch_historical_data(self) -> Dict[str, Any]:
        """
        Fetches historical OHLCV token data for the given token symbol from the specified data source.
        Depending on the historical_data_source, uses the appropriate API endpoint and key.
        """
        print(f"Fetching historical data for token: {self.token_symbol} using {self.historical_data_source}...")
        # Dummy implementation. Integrate with actual Bitquery/DexRabbit API endpoints.
        if self.historical_data_source == "bitquery":
            url = "https://api.bitquery.io/"
            headers = {"X-API-KEY": self.bitquery_api_key}
            response = {}  
        else:
            url = "https://api.dexrabbit.com/"
            headers = {"X-API-KEY": self.dexrabbit_api_key}
            response = {}  

        # Process the response and return structured data
        return response


class SupervisorAgent(BaseModel):
    """
    A supervisor agent that coordinates tasks between the Twitter scraping agent and the token data fetcher agent.
    It runs the Twitter scraping task and the token data fetching task sequentially.
    """
    twitter_agent: TwitterAgent
    token_fetcher_agent: TokenDataFetcherAgent

    def run_twitter_task(self) -> list:
        import os
        import time
        import random
        import pandas as pd
        from datetime import datetime
        import logging

        # Import required functions from twitter_scraper.py
        from twitter.twitter_scraper import TwitterScraper, get_token_list

        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

        # Ensure twitter_data directory exists
        os.makedirs('twitter_data', exist_ok=True)

        tokens = get_token_list()
        if not tokens:
            logger.info("No tokens found in jupiter.csv.")
            return []

        results_df = pd.DataFrame(columns=[
            'token', 'address', 'timestamp', 'tweet_text', 'username',
            'likes', 'retweets', 'sentiment', 'is_meme', 'meme_relevance_score'
        ])

        # Initialize TwitterScraper (separated from TwitterAgent)
        scraper = TwitterScraper()

        for token in tokens:
            logger.info(f"Processing token: {token['name']}")
            try:
                df = scraper.scrape_tweets(query=token['query'], days_back=1, limit=200)
                if not df.empty:
                    df['token'] = token['name']
                    df['address'] = token['address']
                    results_df = pd.concat([results_df, df], ignore_index=True)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f'twitter_data/sentiment_data_{timestamp}.csv'
                    results_df.to_csv(filename, index=False)
                    logger.info(f"Saved twitter results to {filename}")
            except Exception as e:
                logger.error(f"Error processing {token['name']}: {e}")
            time.sleep(random.uniform(5, 10))

        logger.info("Twitter scraping complete.")
        return results_df.to_dict(orient='records')

    def run_token_fetch_task(self) -> dict:
        """
        Runs the token data fetching task using the provided TokenDataFetcherAgent.
        """
        return self.token_fetcher_agent.fetch_historical_data()

    def run_all_tasks(self) -> dict:
        """
        Runs both Twitter scraping and token data fetching tasks, and returns a dictionary with the results.
        """
        twitter_results = self.run_twitter_task()
        token_data = self.run_token_fetch_task()
        return {
            'twitter_results': twitter_results,
            'token_data': token_data
        }
