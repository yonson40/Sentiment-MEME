import pandas as pd
from twitter.BITQUERY_API import BitqueryClient
import os
import logging
from time import sleep

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Create ohlcv_data directory if it doesn't exist
    os.makedirs('ohlcv_data', exist_ok=True)
    
    # Read active tokens
    tokens_df = pd.read_csv('active_tokens.csv')
    
    # Initialize BitQuery client
    client = BitqueryClient()
    
    # USDC address to use as base currency
    usdc_address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    
    # Process each token
    for index, row in tokens_df.iterrows():
        token_address = row['MintAddress']
        symbol = row['Symbol']
        
        # Skip USDC itself
        if token_address == usdc_address:
            logger.info(f"Skipping {symbol} as it's the base currency")
            continue
            
        output_file = f'ohlcv_data/{symbol.lower()}.csv'
        
        # Skip if file already exists
        if os.path.exists(output_file):
            logger.info(f"Skipping {symbol} as data file already exists")
            continue
            
        try:
            # Fetch OHLCV data
            logger.info(f"Fetching data for {symbol}")
            data = client.fetch_ohlcv_data(
                token_address=token_address,
                base_address=usdc_address,
                symbol=symbol,
                interval="1s",  # 1 second intervals
                days_ago=1      # Last 24 hours
            )
            
            if data:
                # Save to CSV
                data.to_csv(output_file, index=False)
                logger.info(f"Saved data for {symbol} to {output_file}")
            else:
                logger.warning(f"No data returned for {symbol}")
                
            # Sleep to avoid rate limiting
            sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {str(e)}")
            continue

if __name__ == "__main__":
    main()
