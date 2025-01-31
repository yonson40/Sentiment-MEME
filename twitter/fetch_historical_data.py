import os
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

def fetch_historical_data():
    client = BitqueryClient()
    output_dir = 'historical_data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Token addresses
    SOL_ADDRESS = "So11111111111111111111111111111111111111112"
    FAFO_ADDRESS = "BP8RUdhLKBL2vgVXc3n7oTSZKWaQVbD8S6QcPaMVBAPo"
    
    # Time intervals to fetch
    intervals = ['5m', '15m', '1h', '4h']
    days_ago = 90  # Fetch 90 days of historical data
    
    for interval in intervals:
        try:
            logger.info(f"Fetching {days_ago} days of historical data with {interval} interval...")
            
            df = client.fetch_ohlcv_data(
                SOL_ADDRESS,
                FAFO_ADDRESS,
                symbol="SOL/FAFO",
                interval=interval,
                days_ago=days_ago
            )
            
            if df is not None and not df.empty:
                filename = f'sol_fafo_{interval}_{days_ago}d.csv'
                filepath = os.path.join(output_dir, filename)
                df.to_csv(filepath, index=False)
                logger.info(f"Saved {len(df)} records to {filename}")
                logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
                logger.info(f"Sample data:\n{df.head()}\n")
            else:
                logger.warning(f"No data available for {interval} interval")
                
        except Exception as e:
            logger.error(f"Error fetching {interval} data: {str(e)}")
            continue

if __name__ == "__main__":
    fetch_historical_data()
