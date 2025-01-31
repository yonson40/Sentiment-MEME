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

def fetch_token_data(days_to_fetch=7):
    """
    Fetch minute-by-minute historical data for multiple tokens
    Args:
        days_to_fetch (int): Number of days of historical data to fetch
    """
    client = BitqueryClient()
    output_dir = 'ohlcv_data/minute_data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Base token (SOL)
    SOL_ADDRESS = "So11111111111111111111111111111111111111112"
    
    # Read token list from jupiter.csv
    try:
        tokens_df = pd.read_csv('jupiter.csv')
        logger.info(f"Found {len(tokens_df)} tokens in jupiter.csv")
    except Exception as e:
        logger.error(f"Error reading jupiter.csv: {e}")
        return
    
    # Process each token
    for idx, row in tokens_df.iterrows():
        token_address = row['address']
        token_symbol = row['symbol']
        
        # Skip SOL as it's our base token
        if token_address == SOL_ADDRESS:
            continue
            
        logger.info(f"Processing token {idx+1}/{len(tokens_df)}: {token_symbol}")
        
        try:
            # Fetch data
            all_data = []
            
            for day in range(days_to_fetch):
                logger.info(f"Fetching day {day + 1} of {days_to_fetch}...")
                
                try:
                    # Fetch OHLCV data
                    data = client.fetch_ohlcv_data(
                        token_address=token_address,
                        base_address=SOL_ADDRESS,
                        symbol=token_symbol,
                        interval="1m",
                        days_ago=day
                    )
                    
                    if data and len(data) > 0:
                        all_data.extend(data)
                    
                    # Sleep to avoid rate limiting
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error fetching data for {token_symbol} day {day}: {e}")
                    continue
            
            if all_data:
                # Convert to DataFrame
                df = pd.DataFrame(all_data)
                
                # Remove duplicates based on timestamp
                df = df.drop_duplicates(subset='timestamp')
                
                # Sort by timestamp
                df = df.sort_values('timestamp')
                
                # Save to CSV
                output_file = os.path.join(output_dir, f"{token_symbol.lower()}_sol_1m_7days.csv")
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {len(df)} records for {token_symbol} to {output_file}")
            else:
                logger.warning(f"No data found for {token_symbol}")
                
        except Exception as e:
            logger.error(f"Error processing {token_symbol}: {e}")
            continue
        
        # Sleep between tokens to avoid overwhelming the API
        time.sleep(2)

if __name__ == "__main__":
    fetch_token_data(7)  # Fetch 7 days of data for each token
