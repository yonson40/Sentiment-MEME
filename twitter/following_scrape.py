import os
import asyncio
from dotenv import load_dotenv
from new_scraper import TwitterScraper

async def main():
    load_dotenv()
    
    # Get credentials from env
    username = os.getenv('TWITTER_USERNAME')
    password = os.getenv('TWITTER_PASSWORD')
    
    if not username or not password:
        raise Exception("Please set TWITTER_USERNAME and TWITTER_PASSWORD in .env file")
    
    print(f"Starting scraper for user: {username}")
    
    # Initialize scraper with cookie caching
    scraper = TwitterScraper(username, password)
    
    try:
        # Login and cache cookies
        if not await scraper.login():
            raise Exception("Failed to login")
        
        print("Successfully logged in!")
        
        # Start monitoring tweets
        await scraper.monitor_following_tweets(username)
    except KeyboardInterrupt:
        print("\nStopping scraper...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await scraper.close()

if __name__ == "__main__":
    asyncio.run(main())