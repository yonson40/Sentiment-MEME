import sqlite3
import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TokenDatabase:
    def __init__(self, db_path='/Users/jeremylevit/Desktop/token_data.sqlite'):
        """Initialize database connection"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to the database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def get_all_tokens(self):
        """Get all tokens from the discovered_tokens table"""
        try:
            query = """
            SELECT 
                address,
                protocol,
                market_cap,
                volume_24h,
                liquidity_usd,
                price_usd,
                discovery_time,
                last_updated,
                status
            FROM discovered_tokens
            """
            
            df = pd.read_sql_query(query, self.conn)
            
            # Convert timestamps to datetime
            df['discovery_time'] = pd.to_datetime(df['discovery_time'])
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            
            logger.info(f"Retrieved {len(df)} tokens")
            return df
            
        except Exception as e:
            logger.error(f"Error getting tokens: {str(e)}")
            return pd.DataFrame()
    
    def get_token_by_address(self, address):
        """Get token data by address"""
        try:
            query = """
            SELECT 
                address,
                protocol,
                market_cap,
                volume_24h,
                liquidity_usd,
                price_usd,
                discovery_time,
                last_updated,
                status
            FROM discovered_tokens
            WHERE address = ?
            """
            
            df = pd.read_sql_query(query, self.conn, params=[address])
            
            if not df.empty:
                # Convert timestamps to datetime
                df['discovery_time'] = pd.to_datetime(df['discovery_time'])
                df['last_updated'] = pd.to_datetime(df['last_updated'])
                logger.info(f"Retrieved token data for {address}")
                return df.iloc[0]
            else:
                logger.warning(f"No token found with address {address}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting token {address}: {str(e)}")
            return None
    
    def get_tokens_by_protocol(self, protocol):
        """Get all tokens for a specific protocol"""
        try:
            query = """
            SELECT 
                address,
                protocol,
                market_cap,
                volume_24h,
                liquidity_usd,
                price_usd,
                discovery_time,
                last_updated,
                status
            FROM discovered_tokens
            WHERE protocol = ?
            """
            
            df = pd.read_sql_query(query, self.conn, params=[protocol])
            
            # Convert timestamps to datetime
            df['discovery_time'] = pd.to_datetime(df['discovery_time'])
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            
            logger.info(f"Retrieved {len(df)} tokens for protocol {protocol}")
            return df
            
        except Exception as e:
            logger.error(f"Error getting tokens for protocol {protocol}: {str(e)}")
            return pd.DataFrame()
    
    def get_active_tokens(self):
        """Get all tokens with status = 'found'"""
        try:
            query = """
            SELECT 
                address,
                protocol,
                market_cap,
                volume_24h,
                liquidity_usd,
                price_usd,
                discovery_time,
                last_updated,
                status
            FROM discovered_tokens
            WHERE status = 'found'
            """
            
            df = pd.read_sql_query(query, self.conn)
            
            # Convert timestamps to datetime
            df['discovery_time'] = pd.to_datetime(df['discovery_time'])
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            
            logger.info(f"Retrieved {len(df)} active tokens")
            return df
            
        except Exception as e:
            logger.error(f"Error getting active tokens: {str(e)}")
            return pd.DataFrame()

    def get_tables(self):
        """Get list of tables in the database"""
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [table[0] for table in self.cursor.fetchall()]
            logger.info(f"Found tables: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {str(e)}")
            return []

    def get_table_schema(self, table_name):
        """Get schema for a specific table"""
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            schema = self.cursor.fetchall()
            columns = [col[1] for col in schema]
            logger.info(f"Schema for {table_name}: {columns}")
            return columns
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {str(e)}")
            return []

def main():
    """Example usage"""
    db = TokenDatabase()
    try:
        # Connect to database
        db.connect()
        
        # Get tables
        print("\nTables in database:")
        tables = db.get_tables()
        for table in tables:
            print(f"- {table}")
            
            # Get schema
            columns = db.get_table_schema(table)
            print(f"  Columns: {columns}")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()
