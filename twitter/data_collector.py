import os
import time
import pandas as pd
from datetime import datetime
import logging
from BITQUERY_API import BitqueryClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self):
        self.client = BitqueryClient()
        self.data_dir = 'collected_data'
        os.makedirs(self.data_dir, exist_ok=True)
        
        # SOL and FAFO addresses
        self.SOL_ADDRESS = "So11111111111111111111111111111111111111112"
        self.FAFO_ADDRESS = "BP8RUdhLKBL2vgVXc3n7oTSZKWaQVbD8S6QcPaMVBAPo"
        
        # Initialize or load existing data
        self.sol_ohlcv_file = os.path.join(self.data_dir, 'sol_fafo_history.csv')
        self.sol_ohlcv_data = self._load_existing_data(self.sol_ohlcv_file)
    
    def _load_existing_data(self, file_path):
        if os.path.exists(file_path):
            try:
                return pd.read_csv(file_path)
            except Exception as e:
                logger.error(f"Error loading {file_path}: {str(e)}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    def collect_data(self):
        try:
            # Fetch SOL/FAFO OHLCV data
            df_ohlcv = self.client.fetch_ohlcv_data(
                self.SOL_ADDRESS, 
                self.FAFO_ADDRESS, 
                symbol="SOL/FAFO", 
                interval="5m"
            )
            
            if df_ohlcv is not None and not df_ohlcv.empty:
                # Add collection timestamp
                df_ohlcv['collected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Append to historical data
                self.sol_ohlcv_data = pd.concat([self.sol_ohlcv_data, df_ohlcv])
                self.sol_ohlcv_data.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
                self.sol_ohlcv_data.to_csv(self.sol_ohlcv_file, index=False)
                logger.info(f"Updated SOL/FAFO data with {len(df_ohlcv)} new records")
                logger.info(f"Latest data:\n{df_ohlcv.tail(1)}")
            else:
                logger.warning("No SOL/FAFO data available")
            
        except Exception as e:
            logger.error(f"Error collecting data: {str(e)}")
    
    def run(self, interval_seconds=300):  # 5 minutes by default
        logger.info(f"Starting data collection every {interval_seconds} seconds...")
        
        while True:
            try:
                self.collect_data()
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                logger.info("Data collection stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in collection loop: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    collector = DataCollector()
    collector.run()
