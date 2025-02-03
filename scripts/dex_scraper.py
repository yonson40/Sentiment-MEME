import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
import logging
import concurrent.futures

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DexScraper:
    def __init__(self):
        """Initialize the DexScraper with API configurations"""
        load_dotenv()
        self.api_key = os.getenv('BITQUERY_API_KEY')
        if not self.api_key:
            raise ValueError("BITQUERY_API_KEY not found in environment variables")
            
        self.endpoint = "https://streaming.bitquery.io/graphql"
        self.headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key
        }

    def get_all_pairs(self):
        """Fetch all available Solana trading pairs from DexRabbit"""
        query = """
        query {
            EVM(dataset: combined) {
                DEXTrades(
                    where: {Trade: {Currency: {SmartContract: {is: "WSOL"}}}}
                    orderBy: {descendingByField: "Block_Time"}
                    limit: 1000
                ) {
                    Trade {
                        Currency {
                            Symbol
                            SmartContract
                        }
                        Side {
                            Currency {
                                Symbol
                                SmartContract
                            }
                        }
                    }
                    Block {
                        Time
                    }
                    volume: sum(of: Trade_Amount)
                }
            }
        }
        """
        
        try:
            response = requests.post(
                self.endpoint,
                json={"query": query},
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            
            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                return []
                
            pairs = []
            seen = set()
            
            for trade in data["data"]["EVM"]["DEXTrades"]:
                base = trade["Trade"]["Currency"]
                quote = trade["Trade"]["Side"]["Currency"]
                
                pair_key = f"{base['Symbol']}/{quote['Symbol']}"
                if pair_key not in seen:
                    pairs.append({
                        "base_symbol": base["Symbol"],
                        "base_address": base["SmartContract"],
                        "quote_symbol": quote["Symbol"],
                        "quote_address": quote["SmartContract"],
                        "pair": pair_key
                    })
                    seen.add(pair_key)
            
            return pairs
            
        except Exception as e:
            logger.error(f"Error fetching pairs: {str(e)}")
            return []

    def construct_query(self, token_address, quote_address, interval_seconds=60):
        """Construct GraphQL query for token data"""
        return """
        query ($base: String!, $quote: String!, $interval: Int!) {
            EVM(dataset: combined) {
                DEXTrades(
                    orderBy: {ascendingByField: "Block_Time"}
                    where: {
                        Trade: {
                            Currency: {SmartContract: {is: $base}},
                            Side: {Currency: {SmartContract: {is: $quote}}}
                        }
                    }
                ) {
                    Block {
                        Time(interval: {count: $interval, in: seconds})
                    }
                    min: minimum(of: Trade_Price)
                    max: maximum(of: Trade_Price)
                    open: first(of: Trade_Price)
                    close: last(of: Trade_Price)
                    volume: sum(of: Trade_Amount)
                    trades: count
                }
            }
        }
        """

    def fetch_pair_data(self, pair_info, interval_seconds=60, retries=3):
        """Fetch trading data for a specific pair"""
        query = self.construct_query(
            pair_info["base_address"],
            pair_info["quote_address"],
            interval_seconds
        )
        
        variables = {
            "base": pair_info["base_address"],
            "quote": pair_info["quote_address"],
            "interval": interval_seconds
        }

        for attempt in range(retries):
            try:
                response = requests.post(
                    self.endpoint,
                    json={"query": query, "variables": variables},
                    headers=self.headers
                )
                response.raise_for_status()
                data = response.json()
                
                if "errors" in data:
                    logger.error(f"GraphQL errors for {pair_info['pair']}: {data['errors']}")
                    continue
                    
                return self._process_response(data, pair_info)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {pair_info['pair']} (attempt {attempt + 1}/{retries}): {str(e)}")
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def _process_response(self, response_data, pair_info):
        """Process the API response into a pandas DataFrame"""
        trades = response_data.get("data", {}).get("EVM", {}).get("DEXTrades", [])
        
        if not trades:
            logger.warning(f"No trade data found for {pair_info['pair']}")
            return pd.DataFrame()
            
        processed_data = []
        for trade in trades:
            processed_data.append({
                'pair': pair_info['pair'],
                'timestamp': trade['Block']['Time'],
                'open': float(trade['open']),
                'high': float(trade['max']),
                'low': float(trade['min']),
                'close': float(trade['close']),
                'volume': float(trade['volume']),
                'trades': int(trade['trades'])
            })
            
        df = pd.DataFrame(processed_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    def save_to_csv(self, df, pair, output_dir="dex_data"):
        """Save the DataFrame to a CSV file"""
        if df.empty:
            logger.warning(f"No data to save for {pair}")
            return
            
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{pair.replace('/', '_')}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        df.to_csv(filepath, index=False)
        logger.info(f"Saved data to {filepath}")
        return filepath

def process_pair(scraper, pair_info):
    """Process a single trading pair"""
    try:
        logger.info(f"Fetching data for {pair_info['pair']}...")
        df = scraper.fetch_pair_data(pair_info)
        
        if not df.empty:
            filepath = scraper.save_to_csv(df, pair_info['pair'])
            logger.info(f"Successfully saved {pair_info['pair']} data to {filepath}")
            logger.info(f"Data summary for {pair_info['pair']}:")
            logger.info(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            logger.info(f"Number of candlesticks: {len(df)}")
            logger.info(f"Price range: {df['low'].min():.6f} to {df['high'].max():.6f}")
            logger.info(f"Total volume: {df['volume'].sum():.2f}")
            return True
        else:
            logger.warning(f"No data retrieved for {pair_info['pair']}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing {pair_info['pair']}: {str(e)}")
        return False

def main():
    scraper = DexScraper()
    
    # Fetch all available pairs
    pairs = scraper.get_all_pairs()
    logger.info(f"Found {len(pairs)} trading pairs")
    
    # Process pairs in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_pair, scraper, pair_info)
            for pair_info in pairs
        ]
        
        # Wait for all tasks to complete
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                completed += 1
                
    logger.info(f"Successfully processed {completed}/{len(pairs)} pairs")

if __name__ == "__main__":
    main()
