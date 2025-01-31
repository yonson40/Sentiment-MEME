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
    # Specific tokens we want to fetch
    specific_tokens = [
        'shy+',
        'mastercard',
        'chad',
        'chinuai',
        'mlg',
        'polar',
        'nazareai',
        'agixt',
        'ross',
        'popcat',
        'relign',
        'butthole'
    ]
    
    # Read active tokens
    tokens_df = pd.read_csv('active_tokens.csv')
    
    # Initialize BitQuery client
    client = BitqueryClient()
    
    # USDC address to use as base currency
    usdc_address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    
    # Process each specific token
    for token_name in specific_tokens:
        # Find the token in our active_tokens.csv
        token_info = tokens_df[tokens_df['Symbol'].str.lower() == token_name.lower()]
        
        if token_info.empty:
            logger.warning(f"Token {token_name} not found in active_tokens.csv")
            continue
            
        token_address = token_info.iloc[0]['MintAddress']
        symbol = token_info.iloc[0]['Symbol']
        output_file = f'ohlcv_data/{token_name.lower()}_ohlcv.csv'
        
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
