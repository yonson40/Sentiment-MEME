import logging
import time

from apscheduler.schedulers.blocking import BlockingScheduler

# Import the necessary modules from our twitter_agent package
from Agents.twitter_agent.database import init_db, insert_tweet
from Agents.twitter_agent.models import Tweet
from Agents.twitter_agent.graph_utils import build_graph

# Import the TwitterScraper from the twitter folder
from twitter.twitter_scraper import TwitterScraper

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def scrape_and_store():
    """Runs the twitter scraper for a specific account and stores the results in the database."""
    logger.info("Starting scraping task for aixbt_agent")
    try:
        scraper = TwitterScraper()
        # Scrape tweets for the account 'aixbt_agent'
        tweets_raw = scraper.scrape_profile(search_username="aixbt_agent")
        if not tweets_raw:
            logger.warning("No tweets were scraped.")
            return

        # Assume the scraped tweets are separated by double newlines\n        tweet_texts = tweets_raw.split("\n\n")

        # Process each tweet text
        for text in tweet_texts:
            if text.strip():
                # Create a Tweet model instance. For demonstration, we use time.time() as a pseudo tweet_id and timestamp.
                tweet = Tweet(
                    tweet_id=str(time.time()),
                    content=text.strip(),
                    timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
                    account="aixbt_agent"
                )
                # Insert the tweet into the database
                insert_tweet(tweet)
        
        # Optionally build/update the relationship graph based on new tweets
        build_graph()

        logger.info("Scraping and storing complete")
    except Exception as e:
        logger.error(f"Error in scraping task: {e}")


def main():
    # Initialize the database
    init_db()

    # Set up the APScheduler to run the scrape_and_store job every hour
    scheduler = BlockingScheduler()
    scheduler.add_job(scrape_and_store, 'interval', hours=1, next_run_time=None)

    logger.info("Twitter Agent scheduler started. Running every hour...")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
