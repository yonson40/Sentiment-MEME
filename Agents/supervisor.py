import schedule 
import time
from twitter_scraper import TwitterScraper
from database import DatabaseManager
from security import SecurityMonitor

class Supervisor:
    def __init__(self):
        self.scrapers = {
            'main_feed': TwitterScraper(),
            'following_feeds': TwitterScraper()
        }
     
    def daily_execution(self):
        try: 
            # Central Auth check
            if not self._authenticate_all():
                raise Exception("Failed to authenticate all scrapers")
            # Scrape data 
            main_feed = self.scraper.scrape_main_feed()
            following_feeds = self.scraper.scrape_following_feeds()
            
            # Process and store
            combined = self.combine_data(main_feed, following_feeds)
            self.db.store(encrypted)
            
            # Backup and cleanup
            self.security.create_backup()
            self.scraper.cleanup()
        
        except Exception as e:
            self.handle_error(e)
    
    def _combine_data(self, *datasets):
        return pd.concat(datasets, ignore_index=True)
    
    def _handle_error(self, error):
        self.security.log_error(error)
        self.db.rollback()
        self.scraper.reset()

if __name__ == "__main__":
    supervisor = Supervisor()
    
    schedule.every().day.at("09:00").do(supervisor.daily_execution)
    
    while True:
        schedule.run_pending()
        time.sleep(60)