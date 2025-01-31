import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataConsolidator:
    def __init__(self, target_db='minute_data/alien_ohlcv/TOKENS.db'):
        """Initialize consolidator"""
        self.target_db = target_db
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to target database"""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.target_db), exist_ok=True)
            
            self.conn = sqlite3.connect(self.target_db)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database: {self.target_db}")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
            
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
            
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            # Create OHLCV table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    token_address TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    source TEXT,
                    collected_at DATETIME,
                    MintAddress TEXT,
                    token_id INTEGER,
                    Symbol TEXT,
                    Name TEXT,
                    UNIQUE(timestamp, token_address)
                )
            """)
            
            # Create token_info table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS token_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    address TEXT UNIQUE,
                    symbol TEXT,
                    name TEXT,
                    decimals INTEGER,
                    total_supply REAL,
                    market_cap REAL,
                    last_updated DATETIME,
                    seq INTEGER,
                    MintAddress TEXT,
                    created_at DATETIME,
                    source TEXT
                )
            """)
            
            self.conn.commit()
            logger.info("Created tables")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
            
    def import_csv_data(self, csv_path, source):
        """Import data from a CSV file"""
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            logger.info(f"Read {len(df)} rows from {csv_path}")
            
            # Determine if this is OHLCV or token info data
            if any(col in df.columns for col in ['open', 'close', 'high', 'low', 'volume']):
                self.import_ohlcv_data(df, source)
            elif any(col in df.columns for col in ['Symbol', 'Name', 'MintAddress']):
                self.import_token_info(df, source)
            else:
                logger.warning(f"Could not determine data type for {csv_path}")
            
        except Exception as e:
            logger.error(f"Error importing {csv_path}: {str(e)}")
    
    def import_sqlite_data(self, db_path, source):
        """Import data from another SQLite database"""
        try:
            # Connect to source database
            source_conn = sqlite3.connect(db_path)
            
            # Get list of tables
            tables = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table'",
                source_conn
            )
            
            for _, row in tables.iterrows():
                table_name = row['name']
                
                # Get table data
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", source_conn)
                logger.info(f"Read {len(df)} rows from {db_path}.{table_name}")
                
                # Add source column if not exists
                if 'source' not in df.columns:
                    df['source'] = f"{source}.{table_name}"
                
                # Try to map to our schema and import
                if any(col in df.columns for col in ['open', 'close', 'high', 'low', 'volume']):
                    self.import_ohlcv_data(df, source)
                elif any(col in df.columns for col in ['symbol', 'name', 'address']):
                    self.import_token_info(df, source)
                    
            source_conn.close()
            
        except Exception as e:
            logger.error(f"Error importing from {db_path}: {str(e)}")
    
    def import_ohlcv_data(self, df, source=None):
        """Import OHLCV data from a DataFrame"""
        try:
            # Add source if not exists
            if 'source' not in df.columns and source:
                df['source'] = source
            
            # Standardize column names
            column_map = {
                'timestamp': ['timestamp', 'time', 'date'],
                'token_address': ['token_address', 'address', 'contract'],
                'open': ['open', 'open_price'],
                'high': ['high', 'high_price'],
                'low': ['low', 'low_price'],
                'close': ['close', 'close_price', 'price'],
                'volume': ['volume', 'volume_24h'],
                'collected_at': ['collected_at', 'collected'],
                'MintAddress': ['MintAddress', 'mint_address'],
                'token_id': ['token_id', 'id'],
                'Symbol': ['Symbol', 'symbol'],
                'Name': ['Name', 'name']
            }
            
            # Rename columns if they exist
            for target, variants in column_map.items():
                for variant in variants:
                    if variant in df.columns:
                        df = df.rename(columns={variant: target})
                        break
            
            # Convert timestamp to datetime if it's not already
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Convert collected_at to datetime if exists
            if 'collected_at' in df.columns:
                df['collected_at'] = pd.to_datetime(df['collected_at'])
            
            # Drop any columns that don't exist in our schema
            table_columns = [col[1] for col in self.cursor.execute("PRAGMA table_info(ohlcv)").fetchall()]
            df = df[[col for col in df.columns if col in table_columns]]
            
            # Insert into database
            df.to_sql('ohlcv', self.conn, if_exists='append', index=False)
            logger.info(f"Imported {len(df)} OHLCV records")
            
        except Exception as e:
            logger.error(f"Error importing OHLCV data: {str(e)}")
    
    def import_token_info(self, df, source=None):
        """Import token info from a DataFrame"""
        try:
            # Add source if not exists
            if 'source' not in df.columns and source:
                df['source'] = source
            
            # Standardize column names
            column_map = {
                'address': ['address', 'token_address', 'contract'],
                'symbol': ['symbol', 'Symbol'],
                'name': ['name', 'Name'],
                'decimals': ['decimals', 'token_decimals'],
                'total_supply': ['total_supply', 'supply'],
                'market_cap': ['market_cap', 'marketcap'],
                'seq': ['seq', 'sequence'],
                'MintAddress': ['MintAddress', 'mint_address'],
                'created_at': ['created_at', 'created']
            }
            
            # Rename columns if they exist
            for target, variants in column_map.items():
                for variant in variants:
                    if variant in df.columns:
                        df = df.rename(columns={variant: target})
                        break
            
            # Add last_updated
            df['last_updated'] = datetime.now()
            
            # Convert timestamps to datetime
            for col in ['created_at', 'last_updated']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Drop any columns that don't exist in our schema
            table_columns = [col[1] for col in self.cursor.execute("PRAGMA table_info(token_info)").fetchall()]
            df = df[[col for col in df.columns if col in table_columns]]
            
            # Insert into database
            df.to_sql('token_info', self.conn, if_exists='append', index=False)
            logger.info(f"Imported {len(df)} token info records")
            
        except Exception as e:
            logger.error(f"Error importing token info: {str(e)}")

def main():
    """Main function to consolidate data"""
    consolidator = DataConsolidator()
    
    try:
        # Connect and create tables
        consolidator.connect()
        consolidator.create_tables()
        
        # List of data sources to import
        sources = [
            # CSV files
            ('collected_data/sol_ohlcv_history.csv', 'sol_history'),
            ('collected_data/sol_price_history.csv', 'sol_price'),
            ('active_tokens.csv', 'active_tokens'),
            ('cleaned_tokens.csv', 'cleaned_tokens'),
            
            # SQLite databases
            ('ohlcv.db', 'ohlcv_db')
        ]
        
        # Import each source
        for source_path, source_name in sources:
            if os.path.exists(source_path):
                logger.info(f"Processing {source_path}")
                
                if source_path.endswith('.csv'):
                    consolidator.import_csv_data(source_path, source_name)
                elif source_path.endswith('.db'):
                    consolidator.import_sqlite_data(source_path, source_name)
            else:
                logger.warning(f"Source not found: {source_path}")
        
        logger.info("Data consolidation complete")
        
    finally:
        consolidator.close()

if __name__ == "__main__":
    main()
