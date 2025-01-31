import sqlite3
import pandas as pd
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_sol_price():
    """Get SOL price data from the database"""
    conn = sqlite3.connect('ohlcv.db')
    
    try:
        # Query for SOL price data
        query = """
        SELECT p.timestamp, p.open, p.high, p.low, p.close, p.volume
        FROM prices p
        JOIN tokens t ON p.token_id = t.id
        WHERE t.symbol = 'SOL'
        ORDER BY p.timestamp DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            # Save to CSV
            df.to_csv('sol_price.csv', index=False)
            logger.info(f"Successfully saved SOL price data to sol_price.csv")
            logger.info(f"Latest data:\n{df.head()}")
        else:
            logger.warning("No SOL price data found in database")
            
    except Exception as e:
        logger.error(f"Error getting SOL price: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    get_sol_price()
