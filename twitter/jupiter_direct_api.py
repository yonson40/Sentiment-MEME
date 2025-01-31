import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JupiterDirectAPI:
    def __init__(self):
        self.base_url = "https://price.jup.ag/v4"
    
    def get_token_price(self, token_address, symbol):
        """
        Get token price from Jupiter directly
        Args:
            token_address: Token mint address
            symbol: Token symbol for logging
        """
        try:
            url = f"{self.base_url}/price?ids={token_address}&vsToken=So11111111111111111111111111111111111111112"
            
            logger.info(f"Fetching price for {symbol} ({token_address})")
            logger.info(f"URL: {url}")
            
            response = requests.get(url)
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response text: {response.text}")
            
            response.raise_for_status()
            data = response.json()
            
            if 'data' not in data or token_address not in data['data']:
                logger.warning(f"No price data found for {symbol} ({token_address})")
                return None
            
            token_data = data['data'][token_address]
            
            # Create DataFrame with price data
            price_data = {
                'timestamp': datetime.fromtimestamp(token_data['timestamp'] / 1000),
                'price': float(token_data['price']),
                'price_change_24h': float(token_data.get('priceChange24h', 0)),
                'volume_24h': float(token_data.get('volume24h', 0))
            }
            
            df = pd.DataFrame([price_data])
            logger.info(f"Successfully got price data for {symbol}: {df.head()}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching price for {symbol} ({token_address}): {str(e)}")
            logger.error("Full error: ", exc_info=True)
            return None

def main():
    # Load tokens from jupiter.csv
    try:
        df = pd.read_csv('jupiter.csv')
        logger.info(f"Loaded {len(df)} tokens from jupiter.csv")
        
        # Display first few tokens for debugging
        logger.info(f"First few tokens:\n{df.head()}")
    except FileNotFoundError:
        logger.error("jupiter.csv not found")
        return
    
    client = JupiterDirectAPI()
    
    # Create directory for price data if it doesn't exist
    os.makedirs('price_data', exist_ok=True)
    
    # Try with just the first token first for testing
    first_token = df.iloc[0]
    symbol = first_token['symbol']
    token_address = first_token['address']
    
    logger.info(f"Testing with first token: {symbol} ({token_address})")
    price_data = client.get_token_price(token_address, symbol)
    
    if price_data is not None and not price_data.empty:
        filename = f"price_data/{symbol}_price.csv"
        price_data.to_csv(filename, index=False)
        logger.info(f"Saved data for {symbol} to {filename}")
        
        # If successful with first token, process the rest
        for _, row in df.iloc[1:].iterrows():
            symbol = row['symbol']
            token_address = row['address']
            
            logger.info(f"Processing {symbol} ({token_address})...")
            price_data = client.get_token_price(token_address, symbol)
            
            if price_data is not None and not price_data.empty:
                filename = f"price_data/{symbol}_price.csv"
                price_data.to_csv(filename, index=False)
                logger.info(f"Saved data for {symbol} to {filename}")
            else:
                logger.warning(f"No data available for {symbol}")
            
            # Add delay to respect rate limits
            time.sleep(0.5)
    else:
        logger.error(f"Failed to get data for first token {symbol}. Please check API access.")

if __name__ == "__main__":
    main()
