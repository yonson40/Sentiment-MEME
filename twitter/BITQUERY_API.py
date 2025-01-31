import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import logging
import http.client
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BitqueryClient:
    def __init__(self):
        self.api_key = os.getenv('BIT_3_TOKEN')
        if not self.api_key:
            raise ValueError("API key is required. Please set BIT_3_TOKEN in your .env file")
        
        self.endpoint = "streaming.bitquery.io"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
    
    def fetch_ohlcv_data(self, token_address, base_address, symbol="", interval="1s", days_ago=1):
        """
        Fetch OHLCV data for a given token pair
        
        Args:
            token_address (str): Token address
            base_address (str): Base token address
            symbol (str): Symbol for logging
            interval (str): Time interval for the data (default "1s" for 1 second)
            days_ago (int): Number of days of historical data to fetch (default 1 to avoid overwhelming with seconds data)
        """
        logger.info(f"Fetching {days_ago} days of {interval} data for {symbol}...")
        
        # Convert interval to seconds for the query
        interval_map = {
            "1s": 1,  # 1 second
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400
        }
        interval_seconds = interval_map.get(interval, 1)  # default to 1s
        
        # Calculate time_ago in ISO format
        time_ago = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        query = """query ($token: String, $base: String, $dataset: dataset_arg_enum, $time_ago: DateTime, $interval: Int) {
  Solana(dataset: $dataset) {
    DEXTradeByTokens(
      orderBy: {ascendingByField: "Block_Time"}
      where: {Trade: {Side: {Amount: {gt: "0"}, Currency: {MintAddress: {is: $token}}}, Currency: {MintAddress: {is: $base}}}, Block: {Time: {after: $time_ago}}}
    ) {
      Block {
        Time(interval: {count: $interval, in: seconds})
      }
      min: quantile(of: Trade_PriceInUSD, level: 0.05)
      max: quantile(of: Trade_PriceInUSD, level: 0.95)
      close: median(of: Trade_PriceInUSD)
      open: median(of: Trade_PriceInUSD)
      volume: sum(of: Trade_Side_AmountInUSD)
    }
  }
}
"""
        
        variables = {
            "token": token_address,
            "base": base_address,
            "dataset": "archive",  
            "time_ago": time_ago,
            "interval": interval_seconds
        }
        
        payload = {
            "query": query,
            "variables": variables
        }
        
        try:
            conn = http.client.HTTPSConnection(self.endpoint)
            conn.request("POST", "/eap", json.dumps(payload), self.headers)
            
            response = conn.getresponse()
            data = response.read()
            response_data = json.loads(data.decode('utf-8'))
            
            logger.info(f"Full API Response: {json.dumps(response_data, indent=2)}")
            
            if 'data' in response_data and 'Solana' in response_data['data']:
                trades = response_data['data']['Solana']['DEXTradeByTokens']
                if not trades:
                    logger.warning(f"No trades found for {symbol}")
                    return pd.DataFrame()  # Return empty DataFrame instead of None
                
                # Process the trades data
                processed_trades = []
                for trade in trades:
                    processed_trade = {
                        'timestamp': trade['Block']['Time'],
                        'open': float(trade['open']),
                        'high': float(trade['max']),
                        'low': float(trade['min']),
                        'close': float(trade['close']),
                        'volume': float(trade['volume'])
                    }
                    processed_trades.append(processed_trade)
                
                df = pd.DataFrame(processed_trades)
                if not df.empty:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                return df
            else:
                logger.error(f"Invalid response format: {response_data}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            return pd.DataFrame()

def main():
    client = BitqueryClient()
    logger.info("Fetching SOL price and OHLCV data...")
    
    # SOL and USDC addresses (using USDC for SOL price)
    SOL_ADDRESS = "So11111111111111111111111111111111111111112"
    USDC_ADDRESS = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    
    try:
        # Fetch SOL/USDC price data
        df_sol_price = client.fetch_ohlcv_data(SOL_ADDRESS, USDC_ADDRESS, symbol="SOL/USDC", interval="1s", days_ago=1)
        
        if df_sol_price is not None and not df_sol_price.empty:
            filename = "sol_price.csv"
            df_sol_price.to_csv(filename, index=False)
            logger.info(f"Successfully saved SOL/USDC price data to {filename}")
            logger.info(f"Latest SOL price data:\n{df_sol_price.tail()}")
        else:
            logger.error("No SOL/USDC price data available")
        
        # Fetch SOL/FAFO OHLCV data
        FAFO_ADDRESS = "BP8RUdhLKBL2vgVXc3n7oTSZKWaQVbD8S6QcPaMVBAPo"
        df_ohlcv = client.fetch_ohlcv_data(SOL_ADDRESS, FAFO_ADDRESS, symbol="SOL/FAFO", interval="1s", days_ago=1)
        
        if df_ohlcv is not None and not df_ohlcv.empty:
            filename = "sol_ohlcv.csv"
            df_ohlcv.to_csv(filename, index=False)
            logger.info(f"Successfully saved SOL/FAFO OHLCV data to {filename}")
            logger.info(f"Latest OHLCV data:\n{df_ohlcv.tail()}")
        else:
            logger.error("No SOL/FAFO OHLCV data available")
            
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")

if __name__ == "__main__":
    main()