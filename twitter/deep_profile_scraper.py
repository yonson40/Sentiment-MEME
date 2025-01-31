import os
import time
import random
import pandas as pd
from datetime import datetime, timedelta
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

class DeepProfileScraper:
    def __init__(self):
        load_dotenv()
        self.username = os.getenv('TWITTER_USERNAME_2')  # Use your @WonsonYo credentials
        self.password = os.getenv('TWITTER_PASSWORD_2')
        self.setup_driver()

    def setup_driver(self):
        """Initialize Chrome WebDriver with appropriate settings"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        
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

    def get_followers_list(self):
        """Get list of your followers"""
        try:
            logger.info("Getting list of followers...")
            self.driver.get(f'https://twitter.com/{self.username}/followers')
            time.sleep(random.uniform(3, 5))
            
            followers = set()
            last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            scroll_attempts = 0
            max_scroll_attempts = 10
            
            while scroll_attempts < max_scroll_attempts:
                # Find all follower elements
                elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="UserCell"]')
                new_followers_found = False
                
                for element in elements:
                    try:
                        username = element.find_element(By.CSS_SELECTOR, '[data-testid="UserCell"] a').get_attribute('href')
                        username = username.split('/')[-1]  # Get username from URL
                        if username not in followers:
                            new_followers_found = True
                            followers.add(username)
                    except:
                        continue
                
                logger.info(f"Found {len(followers)} followers so far")
                
                # Scroll down
                self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(random.uniform(2, 3))
                
                # Calculate new scroll height
                new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                
                # Check if we found new content
                if new_height == last_height and not new_followers_found:
                    scroll_attempts += 1
                    logger.info(f"No new followers found, attempt {scroll_attempts}/{max_scroll_attempts}")
                else:
                    scroll_attempts = 0
                    last_height = new_height
            
            logger.info(f"Found {len(followers)} total followers")
            
            # Save followers list to file
            with open('followers_list.json', 'w') as f:
                json.dump(list(followers), f)
                
            return list(followers)
            
        except Exception as e:
            logger.error(f"Error getting followers list: {str(e)}")
            return []

    def scrape_user_tweets(self, username, months_back=3):
        """Scrape tweets from a specific user going back several months"""
        try:
            logger.info(f"Scraping tweets from @{username}")
            self.driver.get(f'https://twitter.com/{username}')
            time.sleep(random.uniform(2, 3))
            
            tweets = []
            tweet_ids_seen = set()
            scroll_attempts = 0
            max_scroll_attempts = 20  # More attempts since we're going back further
            cutoff_date = datetime.now() - timedelta(days=months_back*30)
            
            # Initial tweet count
            initial_elements = len(self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]'))
            
            while scroll_attempts < max_scroll_attempts:
                # Get current scroll position
                current_position = self.driver.execute_script("return window.pageYOffset;")
                
                # Find all tweet elements
                elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]')
                current_elements = len(elements)
                new_tweets_found = False
                
                for element in elements:
                    try:
                        # Get tweet ID to avoid duplicates
                        tweet_link = element.find_element(By.CSS_SELECTOR, 'a[href*="/status/"]').get_attribute('href')
                        tweet_id = tweet_link.split('/')[-1]
                        
                        if tweet_id in tweet_ids_seen:
                            continue
                            
                        # Get timestamp and check if tweet is too old
                        timestamp = element.find_element(By.CSS_SELECTOR, 'time').get_attribute('datetime')
                        tweet_date = datetime.strptime(timestamp.split('.')[0], '%Y-%m-%dT%H:%M:%S')
                        if tweet_date < cutoff_date:
                            logger.info(f"Found tweet older than {months_back} months, stopping")
                            scroll_attempts = max_scroll_attempts
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
                            
                        try:
                            replies = element.find_element(By.CSS_SELECTOR, '[data-testid="reply"]').text
                            replies = int(replies) if replies else 0
                        except:
                            replies = 0
                            
                        # Check if tweet has media
                        has_image = len(element.find_elements(By.CSS_SELECTOR, '[data-testid="tweetPhoto"]')) > 0
                        has_video = len(element.find_elements(By.CSS_SELECTOR, '[data-testid="videoPlayer"]')) > 0
                        
                        # Check if it's a retweet or reply
                        is_retweet = "Retweeted" in element.text
                        is_reply = len(element.find_elements(By.CSS_SELECTOR, '[data-testid="tweet-reply-context"]')) > 0
                        
                        tweets.append({
                            'username': username,
                            'tweet_id': tweet_id,
                            'timestamp': timestamp,
                            'text': tweet_text,
                            'likes': likes,
                            'retweets': retweets,
                            'replies': replies,
                            'has_image': has_image,
                            'has_video': has_video,
                            'is_retweet': is_retweet,
                            'is_reply': is_reply
                        })
                        
                    except Exception as e:
                        logger.error(f"Error processing tweet: {str(e)}")
                        continue
                
                # Scroll down
                self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(random.uniform(2, 3))  # Give more time for content to load
                
                # Get new scroll position
                new_position = self.driver.execute_script("return window.pageYOffset;")
                new_elements = len(self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]'))
                
                # Check if we've actually moved and loaded new content
                if new_position == current_position and new_elements <= current_elements:
                    if not new_tweets_found:
                        scroll_attempts += 1
                        logger.info(f"No new content after scroll, attempt {scroll_attempts}/{max_scroll_attempts}")
                        # Try scrolling a bit less
                        self.driver.execute_script(f"window.scrollTo(0, {new_position - 300});")
                        time.sleep(1)
                        self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                        time.sleep(random.uniform(2, 3))
                else:
                    scroll_attempts = 0  # Reset counter if we moved or found new tweets
                
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

    def scrape_all_followers(self, months_back=3):
        """Deep scrape of all followers' tweets for sentiment analysis"""
        if not self.login():
            logger.error("Failed to login")
            return
            
        # Get followers list
        followers = self.get_followers_list()
        if not followers:
            logger.error("No followers found")
            return
            
        # Create directory for tweet data
        os.makedirs('sentiment_data', exist_ok=True)
        
        # Create a single CSV for all tweets
        all_tweets = []
        
        # Scrape each follower
        for username in followers:
            try:
                logger.info(f"Scraping tweets from @{username}")
                
                # Get tweets
                tweets = self.scrape_user_tweets(username, months_back)
                if tweets:
                    all_tweets.extend(tweets)
                    logger.info(f"Collected {len(tweets)} tweets from @{username}")
                
                # Random delay between users
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logger.error(f"Error processing @{username}: {str(e)}")
                continue
            
            # Periodically save tweets to avoid losing data
            if len(all_tweets) > 1000:
                self.save_tweets(all_tweets)
                all_tweets = []
        
        # Save any remaining tweets
        if all_tweets:
            self.save_tweets(all_tweets)
        
        self.close()
        logger.info("Finished scraping all followers!")

    def save_tweets(self, tweets):
        """Save tweets to a single CSV file for sentiment analysis"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(tweets)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'sentiment_data/follower_tweets_{timestamp}.csv'
            
            # If file exists, append without headers
            if os.path.exists(filename):
                df.to_csv(filename, mode='a', header=False, index=False)
            else:
                df.to_csv(filename, index=False)
                
            logger.info(f"Saved {len(tweets)} tweets to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving tweets: {str(e)}")

    def close(self):
        """Close the browser"""
        self.driver.quit()

def main():
    try:
        scraper = DeepProfileScraper()
        scraper.scrape_all_followers(months_back=3)
        
    except Exception as e:
        logger.error(f"Main error: {str(e)}")
    finally:
        try:
            scraper.close()
        except:
            pass

if __name__ == "__main__":
    main()
