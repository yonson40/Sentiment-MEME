import os
import time
import random
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from dotenv import load_dotenv
import logging
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FollowingScraper:
    def __init__(self):
        load_dotenv()
        self.username = os.getenv('TWITTER_USERNAME_2')  # Use your @WonsonYo credentials
        self.password = os.getenv('TWITTER_PASSWORD_2')
        self.following_list = []
        self.setup_driver()

    def setup_driver(self):
        """Initialize Chrome WebDriver with appropriate settings"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Uncomment to run headless (no GUI)
        # chrome_options.add_argument('--headless')
        
        # Fix for M1 Macs
        service = Service()
        chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

    def login(self):
        """Log into Twitter"""
        try:
            logger.info("Logging into Twitter...")
            self.driver.get('https://twitter.com/login')
            time.sleep(random.uniform(2, 4))

            # Enter username
            username_input = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'input[autocomplete="username"]'))
            )
            username_input.send_keys(self.username)
            username_input.send_keys(Keys.RETURN)
            time.sleep(random.uniform(1, 2))

            # Enter password
            password_input = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="password"]'))
            )
            password_input.send_keys(self.password)
            password_input.send_keys(Keys.RETURN)
            time.sleep(random.uniform(3, 5))
            
            logger.info("Successfully logged in")
            return True
            
        except Exception as e:
            logger.error(f"Error during login: {str(e)}")
            return False

    def get_following_list(self):
        """Get list of accounts you're following"""
        try:
            logger.info("Getting list of followed accounts...")
            # Navigate to your following page
            self.driver.get(f'https://twitter.com/{self.username}/following')
            time.sleep(random.uniform(3, 5))
            
            following = set()
            last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            
            while True:
                # Find all following elements
                elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="UserCell"]')
                
                for element in elements:
                    try:
                        username = element.find_element(By.CSS_SELECTOR, '[data-testid="UserCell"] a').get_attribute('href')
                        username = username.split('/')[-1]  # Get username from URL
                        following.add(username)
                    except:
                        continue
                
                logger.info(f"Found {len(following)} accounts so far")
                
                # Scroll down
                self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(random.uniform(2, 3))
                
                # Calculate new scroll height
                new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                
                # Break if we've reached the bottom
                if new_height == last_height:
                    break
                    
                last_height = new_height
            
            self.following_list = list(following)
            logger.info(f"Found {len(self.following_list)} total accounts")
            
            # Save following list to file
            with open('following_list.json', 'w') as f:
                json.dump(self.following_list, f)
                
            return self.following_list
            
        except Exception as e:
            logger.error(f"Error getting following list: {str(e)}")
            return []

    def scrape_user_tweets(self, username, days_back=7, limit=100):
        """Scrape tweets from a specific user"""
        try:
            logger.info(f"Scraping tweets from @{username}")
            self.driver.get(f'https://twitter.com/{username}')
            time.sleep(random.uniform(2, 3))
            
            # Load existing tweets to avoid duplicates
            existing_tweets = set()
            if os.path.exists('following_tweets.csv'):
                try:
                    df = pd.read_csv('following_tweets.csv')
                    existing_tweets = set(df['tweet_id'].astype(str))
                    logger.info(f"Loaded {len(existing_tweets)} existing tweets")
                except Exception as e:
                    logger.error(f"Error loading existing tweets: {str(e)}")
            
            tweets = []
            tweet_ids_seen = set()
            scroll_attempts = 0
            max_scroll_attempts = 10  # Stop after 10 unsuccessful scrolls
            cutoff_date = datetime.now() - pd.Timedelta(days=days_back)
            last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            
            while len(tweets) < limit and scroll_attempts < max_scroll_attempts:
                # Find all tweet elements
                elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]')
                new_tweets_found = False
                
                for element in elements:
                    try:
                        # Get tweet ID to avoid duplicates
                        tweet_link = element.find_element(By.CSS_SELECTOR, 'a[href*="/status/"]').get_attribute('href')
                        tweet_id = tweet_link.split('/')[-1]
                        
                        # Skip if we've seen this tweet before
                        if tweet_id in tweet_ids_seen or tweet_id in existing_tweets:
                            continue
                            
                        # Get timestamp and check if tweet is too old
                        timestamp = element.find_element(By.CSS_SELECTOR, 'time').get_attribute('datetime')
                        tweet_date = datetime.strptime(timestamp.split('.')[0], '%Y-%m-%dT%H:%M:%S')
                        if tweet_date < cutoff_date:
                            logger.info(f"Found tweet older than {days_back} days, stopping")
                            scroll_attempts = max_scroll_attempts  # Stop scrolling
                            break
                            
                        tweet_ids_seen.add(tweet_id)
                        new_tweets_found = True
                        
                        # Get tweet text
                        tweet_text = element.find_element(By.CSS_SELECTOR, '[data-testid="tweetText"]').text
                        
                        # Get engagement metrics
                        try:
                            likes = element.find_element(By.CSS_SELECTOR, '[data-testid="like"]').text
                            likes = int(likes) if likes else 0
                        except:
                            likes = 0
                            
                        try:
                            retweets = element.find_element(By.CSS_SELECTOR, '[data-testid="retweet"]').text
                            retweets = int(retweets) if retweets else 0
                        except:
                            retweets = 0
                        
                        tweets.append({
                            'username': username,
                            'tweet_id': tweet_id,
                            'timestamp': timestamp,
                            'text': tweet_text,
                            'likes': likes,
                            'retweets': retweets
                        })
                        
                        if len(tweets) >= limit:
                            break
                            
                    except Exception as e:
                        logger.error(f"Error processing tweet: {str(e)}")
                        continue
                
                # Scroll down
                self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(random.uniform(2, 3))
                
                # Check if scroll was successful
                new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                if new_height == last_height:
                    if not new_tweets_found:
                        scroll_attempts += 1
                        logger.info(f"No new content after scroll, attempt {scroll_attempts}/{max_scroll_attempts}")
                    time.sleep(random.uniform(1, 2))  # Wait a bit longer
                else:
                    scroll_attempts = 0  # Reset counter if we found new content
                    last_height = new_height
                
                # Check for rate limiting
                if "Rate limit exceeded" in self.driver.page_source:
                    wait_time = random.uniform(60, 90)
                    logger.info(f"Rate limit detected. Waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)
            
            logger.info(f"Finished scraping @{username}: {len(tweets)} tweets collected")
            return tweets
            
        except Exception as e:
            logger.error(f"Error scraping tweets from @{username}: {str(e)}")
            return []

    def save_tweets(self, tweets):
        """Save tweets to CSV file"""
        if not tweets:
            logger.warning("No tweets to save")
            return
            
        # Convert tweets to DataFrame
        df = pd.DataFrame(tweets)
        
        # Load existing tweets if file exists
        if os.path.exists('following_tweets.csv'):
            try:
                existing_df = pd.read_csv('following_tweets.csv')
                # Combine new and existing tweets, drop duplicates
                df = pd.concat([existing_df, df], ignore_index=True)
                df = df.drop_duplicates(subset=['tweet_id'], keep='first')
                logger.info(f"Combined with existing tweets, total unique tweets: {len(df)}")
            except Exception as e:
                logger.error(f"Error loading existing tweets: {str(e)}")
        
        # Save to CSV
        df.to_csv('following_tweets.csv', index=False)
        logger.info(f"Saved {len(df)} tweets to following_tweets.csv")

    def scrape_all_following(self, days_back=7, tweets_per_user=50):
        """Scrape tweets from all followed accounts"""
        all_tweets = []
        
        # Load following list if we haven't already
        if not self.following_list:
            try:
                with open('following_list.json', 'r') as f:
                    self.following_list = json.load(f)
            except:
                self.following_list = self.get_following_list()
        
        for username in self.following_list:
            try:
                tweets = self.scrape_user_tweets(username, days_back, tweets_per_user)
                all_tweets.extend(tweets)
                
                # Save progress after each user
                self.save_tweets(all_tweets)
                
                logger.info(f"Collected {len(tweets)} tweets from @{username}")
                logger.info(f"Total tweets collected: {len(all_tweets)}")
                
                # Random delay between users
                time.sleep(random.uniform(5, 10))
                
            except Exception as e:
                logger.error(f"Error processing user @{username}: {str(e)}")
                continue
        
        return all_tweets

    def close(self):
        """Close the browser"""
        self.driver.quit()

def main():
    scraper = FollowingScraper()
    
    try:
        # Login
        if not scraper.login():
            logger.error("Failed to login")
            return
            
        # Get following list
        following = scraper.get_following_list()
        logger.info(f"Found {len(following)} accounts you're following")
        
        # Scrape tweets from all following
        tweets = scraper.scrape_all_following(days_back=7, tweets_per_user=50)
        
        # Save final results
        scraper.save_tweets(tweets)
        
        logger.info(f"Finished! Collected {len(tweets)} tweets total")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
