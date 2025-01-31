from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from textblob import TextBlob
import logging
import time
import random
import traceback

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TwitterScraper:
    def __init__(self):
        load_dotenv()
        self.setup_driver()
        self.login_attempts = 0
        self.max_retries = 3
        self.rate_limit_delay = random.uniform(2, 5)
        self.session_start_time = datetime.now()
        self.tweets_scraped = 0
        
    def setup_driver(self):
        """Set up Chrome driver with appropriate options"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Add additional options for better scraping
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            # Try to use the system Chrome installation
            service = Service()
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            logger.error(f"Error with default ChromeDriver: {str(e)}")
            try:
                # Fallback to WebDriver Manager
                service = Service(ChromeDriverManager(os_type="mac_arm64").install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                logger.error(f"Error with WebDriver Manager: {str(e)}")
                raise
        
        # Set up additional browser configurations
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def get_sentiment(self, text):
        """Analyze sentiment of text using TextBlob"""
        try:
            analysis = TextBlob(text)
            return analysis.sentiment.polarity
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {str(e)}")
            return 0
            
    def scroll_page(self, scroll_count=5):
        """Scroll the page to load more tweets"""
        for _ in range(scroll_count):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1, 2))  # Random delay between scrolls
            
    def extract_tweet_data(self, tweet_element):
        """Extract data from a tweet element"""
        try:
            # Find tweet text
            text_element = tweet_element.find_element(By.CSS_SELECTOR, '[data-testid="tweetText"]')
            text = text_element.text
            
            # Get timestamp
            time_element = tweet_element.find_element(By.CSS_SELECTOR, 'time')
            timestamp = time_element.get_attribute('datetime')
            
            # Get metrics (likes, retweets)
            metrics = tweet_element.find_elements(By.CSS_SELECTOR, '[data-testid$="-count"]')
            likes = retweets = 0
            
            for metric in metrics:
                metric_id = metric.get_attribute('data-testid')
                value = int(metric.text or 0)
                if 'like' in metric_id:
                    likes = value
                elif 'retweet' in metric_id:
                    retweets = value
                    
            # Get username
            username = tweet_element.find_element(By.CSS_SELECTOR, '[data-testid="User-Name"]').text.split('\n')[0]
            
            # Calculate sentiment
            sentiment = self.get_sentiment(text)
            
            return {
                'timestamp': timestamp,
                'text': text,
                'username': username,
                'likes': likes,
                'retweets': retweets,
                'sentiment': sentiment
            }
            
        except Exception as e:
            logger.error(f"Error extracting tweet data: {str(e)}")
            return None
            
    def login_to_twitter(self):
        """Login to Twitter using credentials from .env"""
        try:
            username = os.getenv('TWITTER_USERNAME')
            password = os.getenv('TWITTER_PASSWORD')
            
            if not username or not password:
                logger.error("Twitter credentials not found in .env file")
                return False
                
            logger.info("Starting Twitter login process")
            self.driver.get('https://twitter.com/i/flow/login')
            time.sleep(random.uniform(3, 5))
            
            # Wait for and enter username
            logger.info("Looking for username field")
            username_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'input[autocomplete="username"]'))
            )
            logger.info("Found username field, entering username")
            for char in username:
                username_input.send_keys(char)
                time.sleep(random.uniform(0.1, 0.3))
            time.sleep(random.uniform(1, 2))
            
            # Click the 'Next' button
            logger.info("Looking for Next button")
            next_button = self.driver.find_element(By.XPATH, "//span[text()='Next']")
            next_button.click()
            time.sleep(random.uniform(2, 3))
            
            # Handle possible "Unusual login activity" screen
            try:
                unusual_activity = WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Enter your phone number or username')]"))
                )
                logger.info("Detected unusual activity screen")
                username_verify = self.driver.find_element(By.CSS_SELECTOR, 'input[autocomplete="username"]')
                username_verify.send_keys(username)
                username_verify.send_keys(Keys.RETURN)
                time.sleep(random.uniform(2, 3))
            except Exception as e:
                logger.info("No unusual activity screen detected")
                
            # Wait for and enter password
            logger.info("Looking for password field")
            password_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="password"]'))
            )
            logger.info("Found password field, entering password")
            for char in password:
                password_input.send_keys(char)
                time.sleep(random.uniform(0.1, 0.3))
            time.sleep(random.uniform(1, 2))
            
            # Click login button
            logger.info("Submitting login form")
            password_input.send_keys(Keys.RETURN)
            time.sleep(random.uniform(3, 5))
            
            # Verify login success
            try:
                # Check for home timeline
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="primaryColumn"]'))
                )
                logger.info("Successfully logged in to Twitter")
                return True
            except Exception as e:
                # Check for error messages
                try:
                    error_message = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="error-detail"]').text
                    logger.error(f"Login error message: {error_message}")
                except:
                    logger.error("Could not find specific error message")
                    
                # Log the page source for debugging
                logger.error("Login verification failed")
                logger.error(f"Current URL: {self.driver.current_url}")
                logger.error(f"Page source preview: {self.driver.page_source[:1000]}")  # Log first 1000 chars of page source
                return False
                
        except Exception as e:
            logger.error(f"Error during login: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            self.login_attempts += 1
            if self.login_attempts < self.max_retries:
                logger.info(f"Retrying login (attempt {self.login_attempts + 1}/{self.max_retries})")
                time.sleep(random.uniform(2, 4))
                return self.login_to_twitter()
            return False
            
    def is_rate_limited(self):
        """Check if we're hitting rate limits based on tweets scraped and time"""
        tweets_per_hour_limit = 300  # Adjust as needed
        session_duration = (datetime.now() - self.session_start_time).total_seconds() / 3600
        
        if session_duration > 0:
            tweets_per_hour = self.tweets_scraped / session_duration
            return tweets_per_hour > tweets_per_hour_limit
            
        return False
        
    def handle_rate_limit(self):
        """Handle rate limiting by implementing exponential backoff"""
        if self.is_rate_limited():
            wait_time = self.rate_limit_delay * (2 ** (self.tweets_scraped // 100))
            wait_time = min(wait_time, 300)  # Cap at 5 minutes
            logger.info(f"Rate limit detected. Waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
            
    def scrape_tweets(self, query, days_back=1, limit=100):
        """
        Scrape tweets for a given query using Selenium with improved handling
        """
        tweets_list = []
        retry_count = 0
        
        try:
            logger.info(f"Starting tweet scraping for query: {query}")
            
            # Login first
            if not self.login_to_twitter():
                logger.error("Failed to login to Twitter")
                return pd.DataFrame()
                
            # Add meme-related keywords to the query
            meme_keywords = ['meme', 'viral', 'trending', 'funny', 'lol', 'wojak', 'pepe']
            enhanced_query = f"{query} OR ({query} ({' OR '.join(meme_keywords)}))"
            logger.info(f"Enhanced query: {enhanced_query}")
            
            # Construct search URL with advanced filters
            search_url = f"https://twitter.com/search?q={enhanced_query}%20lang%3Aen&src=typed_query&f=live"
            logger.info(f"Search URL: {search_url}")
            
            self.driver.get(search_url)
            logger.info("Loaded search page")
            time.sleep(random.uniform(2, 4))
            
            try:
                # Wait for tweets to load
                wait = WebDriverWait(self.driver, 10)
                tweet_elements = wait.until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-testid="tweet"]'))
                )
                logger.info(f"Found {len(tweet_elements)} initial tweets")
            except Exception as e:
                logger.error(f"Error waiting for tweets: {str(e)}")
                logger.info(f"Page source: {self.driver.page_source[:500]}...")  # Log first 500 chars of page source
                return pd.DataFrame()
            
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            logger.info(f"Initial page height: {last_height}")
            
            while len(tweets_list) < limit:
                self.handle_rate_limit()
                
                # Find all tweets on the page
                tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]')
                logger.info(f"Found {len(tweet_elements)} tweets on page")
                
                # Process new tweets
                for tweet in tweet_elements:
                    if len(tweets_list) >= limit:
                        break
                        
                    tweet_data = self.extract_tweet_data(tweet)
                    if tweet_data:
                        # Add meme detection
                        tweet_data['is_meme'] = any(keyword in tweet_data['text'].lower() for keyword in meme_keywords)
                        tweets_list.append(tweet_data)
                        self.tweets_scraped += 1
                        logger.info(f"Processed tweet {len(tweets_list)}/{limit}")
                        
                # Scroll with dynamic wait
                self.scroll_page(1)
                logger.info("Scrolled page")
                
                # Check if we've reached the bottom
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                logger.info(f"New page height: {new_height}")
                
                if new_height == last_height:
                    retry_count += 1
                    logger.info(f"No new content loaded, retry {retry_count}/3")
                    if retry_count > 3:
                        logger.info("Reached end of available tweets")
                        break
                else:
                    retry_count = 0
                    last_height = new_height
                    
                # Add random delay
                delay = random.uniform(1, 3)
                logger.info(f"Waiting {delay:.2f} seconds")
                time.sleep(delay)
                
            logger.info(f"Collected {len(tweets_list)} tweets")
            df = pd.DataFrame(tweets_list)
            
            # Add additional sentiment analysis for meme context
            if not df.empty:
                df['meme_relevance_score'] = df.apply(lambda row: 
                    row['sentiment'] * (2 if row['is_meme'] else 1) * 
                    (1 + (row['likes'] + row['retweets']) / 1000), axis=1)
                logger.info("Added meme relevance scores")
                
            return df
            
        except Exception as e:
            logger.error(f"Error scraping tweets: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return pd.DataFrame()
            
    def save_tweets(self, df, token_name):
        """Save tweets to CSV file"""
        try:
            # Create directory if it doesn't exist
            os.makedirs('twitter_data', exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'twitter_data/{token_name}_{timestamp}.csv'
            
            # Save to CSV
            df.to_csv(filename, index=False)
            logger.info(f"Saved tweets to {filename}")
            
            return filename
        except Exception as e:
            logger.error(f"Error saving tweets: {str(e)}")
            return None

def get_last_processed_token():
    """Get the last successfully processed token from the most recent data file"""
    try:
        # Find the most recent sentiment data file
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'twitter_data')
        if not os.path.exists(data_dir):
            return None
            
        files = [f for f in os.listdir(data_dir) if f.startswith('sentiment_data_')]
        if not files:
            return None
            
        latest_file = max(files, key=lambda x: x.split('_')[2])
        latest_path = os.path.join(data_dir, latest_file)
        
        # Read the file and get the last token
        df = pd.read_csv(latest_path)
        if df.empty:
            return None
            
        # Get unique tokens and their counts
        token_counts = df['token'].value_counts()
        if token_counts.empty:
            return None
            
        # Get the last fully processed token (one with significant data)
        min_tweets = 10  # Minimum tweets to consider a token as fully processed
        for token in token_counts.index:
            if token_counts[token] >= min_tweets:
                logger.info(f"Found last processed token: {token} with {token_counts[token]} tweets")
                return token.lower()  # Convert to lowercase for consistent comparison
                
        return None
        
    except Exception as e:
        logger.error(f"Error getting last processed token: {str(e)}")
        return None

def get_token_list():
    """Get list of tokens from jupiter.csv"""
    tokens = []
    base_dir = os.path.dirname(os.path.dirname(__file__))
    jupiter_path = os.path.join(base_dir, 'jupiter.csv')
    logger.info(f"Reading tokens from: {jupiter_path}")
    
    try:
        # Read CSV with string type for symbol and name columns
        df = pd.read_csv(jupiter_path, dtype={'symbol': str, 'name': str})
        logger.info(f"Found {len(df)} tokens in jupiter.csv")
        
        # Start with SOL
        sol_row = df[df['symbol'].str.upper() == 'SOL'].iloc[0]
        tokens.append({
            'name': 'sol',
            'query': '$SOL',
            'address': sol_row['address']
        })
        logger.info("Added SOL token")
        
        # Add other tokens (excluding stablecoins and wrapped tokens)
        exclude_symbols = ['USDC', 'USDT', 'PYUSD', 'USN', 'DAI', 'WBTC', 'ETH', 'BTC', 'SUSHI', 'ALEPH']
        exclude_prefixes = ['so', 'w', 'st', 'x', 'ms', 'bs', 'hs', 'stake', 'wrapped']
        
        for _, row in df.iterrows():
            try:
                symbol = str(row['symbol']).strip()
                name = str(row['name']).strip()
                
                # Skip if it's in exclude list or starts with excluded prefixes
                if (symbol.upper() in exclude_symbols or 
                    any(symbol.lower().startswith(prefix) for prefix in exclude_prefixes)):
                    logger.info(f"Skipping token: {symbol} ({name})")
                    continue
                    
                # Skip if it's a wrapped or synthetic version
                if any(keyword in name.lower() for keyword in ['wrapped', 'synthetic', 'sollet', 'wormhole']):
                    logger.info(f"Skipping wrapped/synthetic token: {symbol} ({name})")
                    continue
                    
                tokens.append({
                    'name': symbol.lower(),
                    'query': f'${symbol}',
                    'address': row['address']
                })
                logger.info(f"Added token: {symbol} ({name})")
                
            except Exception as e:
                logger.error(f"Error processing row: {row}")
                logger.error(str(e))
                continue
            
        logger.info(f"Total tokens to track: {len(tokens)}")
        logger.info("Token list: " + ", ".join([t['query'] for t in tokens]))
        
        return tokens
        
    except Exception as e:
        logger.error(f"Error reading jupiter.csv: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return []

def main():
    # Initialize scraper
    scraper = TwitterScraper()
    
    # Get tokens from jupiter.csv
    tokens = get_token_list()
    
    logger.info(f"Found {len(tokens)} tokens to track")
    
    # Create directory for results if it doesn't exist
    os.makedirs('twitter_data', exist_ok=True)
    
    # Get last processed token
    last_token = get_last_processed_token()
    
    # Initialize or load results DataFrame
    results_df = pd.DataFrame(columns=[
        'token', 'address', 'timestamp', 'tweet_text', 'username',
        'likes', 'retweets', 'sentiment', 'is_meme', 'meme_relevance_score'
    ])
    
    # Load existing results if available
    if last_token:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'twitter_data')
        latest_file = max(
            [f for f in os.listdir(data_dir) if f.startswith('sentiment_data_')],
            key=lambda x: x.split('_')[2]
        )
        results_df = pd.read_csv(os.path.join(data_dir, latest_file))
        logger.info(f"Loaded {len(results_df)} existing results")
    
    # Find starting point
    start_idx = 0
    if last_token:
        for idx, token in enumerate(tokens):
            if token['name'].lower() == last_token.lower():
                start_idx = idx + 1  # Start from next token
                break
        logger.info(f"Resuming from token index {start_idx}")
    
    # Skip tokens we've already processed
    tokens_to_process = tokens[start_idx:]
    logger.info(f"Remaining tokens to process: {len(tokens_to_process)}")
    logger.info("Next tokens: " + ", ".join([t['query'] for t in tokens_to_process[:5]]))
    
    # Scrape tweets for each token
    for token in tokens_to_process:
        logger.info(f"Processing {token['name']}")
        
        try:
            # Scrape tweets
            df = scraper.scrape_tweets(
                query=token['query'],
                days_back=1,
                limit=200  # Increased limit for better sentiment analysis
            )
            
            if not df.empty:
                # Add token info to each row
                df['token'] = token['name']
                df['address'] = token['address']
                
                # Append to results
                results_df = pd.concat([results_df, df], ignore_index=True)
                
                # Save intermediate results
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'twitter_data/sentiment_data_{timestamp}.csv'
                results_df.to_csv(filename, index=False)
                
                logger.info(f"Saved results to {filename}")
                logger.info(f"Current statistics for {token['name']}:")
                logger.info(f"- Tweets collected: {len(df)}")
                logger.info(f"- Average sentiment: {df['sentiment'].mean():.2f}")
                logger.info(f"- Meme relevance: {df['meme_relevance_score'].mean():.2f}")
                logger.info(f"- Meme ratio: {df['is_meme'].mean():.2%}")
            
        except Exception as e:
            logger.error(f"Error processing {token['name']}: {str(e)}")
            # Save progress before exiting on error
            final_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            final_filename = f'twitter_data/sentiment_data_{final_timestamp}.csv'
            results_df.to_csv(final_filename, index=False)
            logger.info(f"Saved progress to {final_filename}")
            raise  # Re-raise the exception to stop processing
            
        # Add delay between tokens to avoid rate limiting
        time.sleep(random.uniform(5, 10))
        
    # Save final results
    final_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    final_filename = f'twitter_data/final_sentiment_data_{final_timestamp}.csv'
    results_df.to_csv(final_filename, index=False)
    logger.info(f"Saved final results to {final_filename}")

if __name__ == "__main__":
    main()
