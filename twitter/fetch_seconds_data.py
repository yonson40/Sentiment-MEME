import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from BITQUERY_API import BitqueryClient
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_seconds_data(days_to_fetch=7):
    """
    Fetch minute-by-minute historical data for the past specified days
    Args:
        days_to_fetch (int): Number of days of historical data to fetch
    """
    client = BitqueryClient()
    output_dir = 'minute_data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Token addresses
    SOL_ADDRESS = "So11111111111111111111111111111111111111112"
    FAFO_ADDRESS = "BP8RUdhLKBL2vgVXc3n7oTSZKWaQVbD8S6QcPaMVBAPo"
    
    all_data = []
    
    # Fetch data day by day to avoid overwhelming the API
    for day in range(days_to_fetch):
        try:
            logger.info(f"Fetching day {day + 1} of {days_to_fetch}...")
            
            df = client.fetch_ohlcv_data(
                SOL_ADDRESS,
                FAFO_ADDRESS,
                symbol="SOL/FAFO",
                interval="1m",  # 1-minute intervals
                days_ago=day
            )
            
            if df is not None and not df.empty:
                all_data.append(df)
                logger.info(f"Fetched {len(df)} records for day -{day}")
                logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
                logger.info(f"Sample data:\n{df.head()}\n")
                
                # Sleep to avoid rate limiting
                time.sleep(2)
            else:
                logger.warning(f"No data available for day -{day}")
                
        except Exception as e:
            logger.error(f"Error fetching data for day -{day}: {str(e)}")
            continue
    
    if all_data:
        # Combine all data
        combined_df = pd.concat(all_data)
        combined_df = combined_df.sort_values('timestamp')
        combined_df = combined_df.drop_duplicates()
        
        # Save combined data
        filename = f'sol_fafo_1m_{days_to_fetch}days.csv'
        filepath = os.path.join(output_dir, filename)
        combined_df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(combined_df)} total records to {filename}")
        logger.info(f"Full date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    else:
        logger.error("No data was collected")

if __name__ == "__main__":
    # Fetch 7 days of minute-by-minute data
    fetch_seconds_data(7)
