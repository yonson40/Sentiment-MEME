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
        
        # Add persistent element for visibility check
        self.driver.execute_script("document.body.innerHTML += '<div id=\"scraperActive\" style=\"display:none;\">ACTIVE</div>'")
        
    def check_browser_visible(self):
        """Verify browser window is actually open"""
        try:
            return self.driver.find_element(By.ID, "scraperActive").is_displayed()
        except:
            return False
            
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
        """Extract structured data from tweet HTML"""
        try:
            # Get raw HTML first
            raw_html = tweet_element.get_attribute('outerHTML')
            
            # Extract individual components
            timestamp = tweet_element.find_element(By.CSS_SELECTOR, 'time').get_attribute('datetime')
            username = tweet_element.find_element(By.CSS_SELECTOR, '[data-testid="User-Name"]').text.split('\n')[0]
            text = tweet_element.find_element(By.CSS_SELECTOR, '[data-testid="tweetText"]').text
            
            # Get engagement metrics
            metrics = {
                'likes': 0,
                'retweets': 0,
                'replies': 0
            }
            
            # Extract metrics from buttons
            buttons = tweet_element.find_elements(By.CSS_SELECTOR, '[role="button"]')
            for btn in buttons:
                aria_label = btn.get_attribute('aria-label').lower()
                if 'reply' in aria_label:
                    metrics['replies'] = int(btn.text) if btn.text else 0
                elif 'like' in aria_label:
                    metrics['likes'] = int(btn.text) if btn.text else 0
                elif 'retweet' in aria_label:
                    metrics['retweets'] = int(btn.text) if btn.text else 0

            return {
                'timestamp': timestamp,
                'username': username,
                'text': text,
                'likes': metrics['likes'],
                'retweets': metrics['retweets'],
                'replies': metrics['replies'],
                'raw_html': raw_html
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
            
    def scrape_tweets(self, query, days_back=365, limit=50000):
        try:
            search_url = self._build_search_url(query, days_back)
            self.driver.get(search_url)
            
            tweets = []
            scroll_attempts = 0
            max_attempts = 1000
            batch_size = 200
            
            while len(tweets) < limit and scroll_attempts < max_attempts:
                if self._handle_rate_limits():
                    scroll_attempts += 10
                    continue
                new_tweets = self._harvest_tweets(batch_size)
                tweets.extend(new_tweets)
                if len(tweets) % 1000 == 0:
                    logger.info(f"Collected {len(tweets)}/{limit} tweets")
                    self._save_checkpoint(tweets)
                self._smart_scroll()
                scroll_attempts += 1
                current_speed = len(tweets) / (scroll_attempts + 1)
                if current_speed < 5:
                    self._slow_down_scraping()
            return pd.DataFrame(tweets[:limit])
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            return pd.DataFrame()
            
    def _build_search_url(self, query, days_back):
        return f"https://twitter.com/search?q={query} min_replies:100 min_faves:5000 since:{self._get_past_date(days_back)}"
        
    def _get_past_date(self, days_back):
        past_date = datetime.now() - timedelta(days=days_back)
        return past_date.strftime('%Y-%m-%d')
        
    def _process_tweet_batch(self):
        try:
            # Find all tweets on the page
            tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]')
            logger.info(f"Found {len(tweet_elements)} tweets on page")
            
            # Process new tweets
            tweets = []
            for tweet in tweet_elements:
                tweet_data = self.extract_tweet_data(tweet)
                if tweet_data:
                    tweets.append(tweet_data)
                    self.tweets_scraped += 1
                    logger.info(f"Processed tweet {len(tweets)}/{len(tweet_elements)}")
                    
            return tweets
        except Exception as e:
            logger.error(f"Error processing tweet batch: {str(e)}")
            return []
            
    def _save_checkpoint(self, tweets):
        try:
            # Save tweets to CSV with proper structure
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'twitter_data/checkpoint_{timestamp}.csv'
            
            # Select and order columns
            df = pd.DataFrame(tweets)
            df = df[[
                'timestamp',
                'username',
                'text', 
                'likes',
                'retweets',
                'replies',
                'raw_html'
            ]]
            
            df.to_csv(filename, index=False)
            logger.info(f"Saved checkpoint to {filename}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
            
    def _evade_detection_scroll(self):
        try:
            # Scroll with dynamic wait
            self.scroll_page(1)
            logger.info("Scrolled page")
        except Exception as e:
            logger.error(f"Error evading detection: {str(e)}")
            
    def _handle_rate_limits(self):
        if self.is_rate_limited():
            wait_time = self.rate_limit_delay * (2 ** (self.tweets_scraped // 100))
            wait_time = min(wait_time, 300)  # Cap at 5 minutes
            logger.info(f"Rate limit detected. Waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
            return True
        return False
            
    def _harvest_tweets(self, batch_size):
        try:
            # Find all tweets on the page
            tweet_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]')
            logger.info(f"Found {len(tweet_elements)} tweets on page")
            
            # Process new tweets
            tweets = []
            for tweet in tweet_elements[:batch_size]:
                tweet_data = self.extract_tweet_data(tweet)
                if tweet_data:
                    tweets.append(tweet_data)
                    self.tweets_scraped += 1
                    logger.info(f"Processed tweet {len(tweets)}/{batch_size}")
                    
            return tweets
        except Exception as e:
            logger.error(f"Error harvesting tweets: {str(e)}")
            return []
            
    def _smart_scroll(self):
        try:
            # Scroll with dynamic wait
            self.scroll_page(1)
            logger.info("Scrolled page")
        except Exception as e:
            logger.error(f"Error smart scrolling: {str(e)}")
            
    def _slow_down_scraping(self):
        try:
            # Slow down scraping
            time.sleep(random.uniform(10, 30))
            logger.info("Slowed down scraping")
        except Exception as e:
            logger.error(f"Error slowing down scraping: {str(e)}")
            
    def save_tweets(self, df, token_name):
        """Save tweets to CSV with proper structure"""
        try:
            os.makedirs('twitter_data', exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'twitter_data/scraped_tweets_{timestamp}.csv'
            
            # Select and order columns
            df = df[[
                'timestamp',
                'username',
                'text', 
                'likes',
                'retweets',
                'replies',
                'raw_html'
            ]]
            
            df.to_csv(filename, index=False)
            logger.info(f"Saved structured tweets to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error saving tweets: {str(e)}")
            return None

    def login_account(self):
        """Log in to x.com using TWITTER_USERNAME_2 and TWITTER_PASSWORD_2 from environment variables."""
        username = os.getenv("TWITTER_USERNAME_2")
        password = os.getenv("TWITTER_PASSWORD_2")
        if not username or not password:
            logger.info("No login credentials provided; skipping login.")
            return False
        self.driver.get("https://x.com/login")
        time.sleep(3)
        try:
            username_field = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, "text"))
            )
            username_field.send_keys(username)
            username_field.send_keys(Keys.RETURN)
            time.sleep(3)
            password_field = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, "password"))
            )
            password_field.send_keys(password)
            password_field.send_keys(Keys.RETURN)
            time.sleep(5)
            logger.info("Logged in successfully")
            return True
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    def scrape_profile(self, search_username="aixbt_agent"):
        """Log in (if credentials provided), navigate to https://x.com, search for the given username, click the profile, and scrape all tweets via infinite scrolling."""
        # Attempt login if credentials are provided
        username = os.getenv("TWITTER_USERNAME_2")
        password = os.getenv("TWITTER_PASSWORD_2")
        if username and password:
            logged_in = self.login_account()
            if not logged_in:
                logger.error("Login failed; proceeding without login.")

        try:
            # Navigate to home page
            self.driver.get("https://x.com")
            time.sleep(random.uniform(3, 5))  

            # Locate the search box with fallback if primary locator is not found
            try:
                search_box = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Search X']"))
                )
            except Exception as e:
                logger.info("Primary search box locator not found, trying alternative locator.")
                search_box = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//input[contains(@aria-label, 'Search')]"))
                )
            search_box.clear()
            search_box.send_keys(search_username)
            search_box.send_keys(Keys.RETURN)
            time.sleep(random.uniform(3, 5))  

            # Click the profile link from search results
            profile_link = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, f"//a[contains(@href, '/{search_username}')]"))
            )
            profile_link.click()
            time.sleep(random.uniform(3, 5))

            # Infinite scroll loop with a fallback for "Show more" button and extended scroll attempts
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            max_scroll_attempts = 5
            while scroll_attempts < max_scroll_attempts:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(2, 4))
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    try:
                        show_more = self.driver.find_element(By.XPATH, "//span[text()='Show more']")
                        if show_more.is_displayed():
                            show_more.click()
                            time.sleep(random.uniform(2, 4))
                            new_height = self.driver.execute_script("return document.body.scrollHeight")
                        else:
                            scroll_attempts += 1
                    except Exception as e:
                        scroll_attempts += 1
                else:
                    scroll_attempts = 0
                last_height = new_height

            # Extract all tweet texts from the loaded page
            tweets = self.driver.find_elements(By.XPATH, '//article')
            tweet_texts = [tweet.text for tweet in tweets if tweet.text]
            full_text = "\n\n".join(tweet_texts)
            with open("aixbt_agent_full_tweets.txt", "w", encoding="utf-8") as f:
                f.write(full_text)
            logger.info(f"Scraped {len(tweet_texts)} tweets and saved to aixbt_agent_full_tweets.txt")
            return full_text
        except Exception as e:
            logger.error(f"Error scraping profile: {str(e)}")
            traceback.print_exc()
            return ""

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
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "profile":
        profile_url = sys.argv[2] if len(sys.argv) > 2 else "https://x.com/aixbt_agent"
        scraper = TwitterScraper()
        scraper.scrape_profile(profile_url)
        return

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
        'likes', 'retweets', 'replies', 'sentiment', 'is_meme', 'meme_relevance_score'
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
                days_back=365,
                limit=50000
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
