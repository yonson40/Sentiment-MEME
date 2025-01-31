import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime
import glob

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OHLCVConsolidator:
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
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    token_name TEXT,
                    source_file TEXT
                )
            """)
            
            # Create index on timestamp and token_name
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_ohlcv_token_time 
                ON ohlcv(token_name, timestamp)
            """)
            
            self.conn.commit()
            logger.info("Created tables and indices")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
    
    def get_token_name(self, file_path):
        """Extract token name from file path"""
        filename = os.path.basename(file_path)
        # Remove _seconds, _ohlcv, etc. suffixes and file extension
        token_name = filename.replace('_seconds', '').replace('_ohlcv', '').replace('.csv', '')
        return token_name.upper()

    def import_csv_file(self, file_path):
        """Import OHLCV data from a CSV file"""
        try:
            # Get token name from file name
            token_name = self.get_token_name(file_path)
            
            # Read CSV
            df = pd.read_csv(file_path)
            logger.info(f"Read {len(df)} rows from {file_path}")
            
            # Standardize column names (case-insensitive)
            column_map = {
                'timestamp': ['timestamp', 'time', 'date', 'Time'],
                'open': ['open', 'Open', 'open_price'],
                'high': ['high', 'High', 'high_price'],
                'low': ['low', 'Low', 'low_price'],
                'close': ['close', 'Close', 'close_price'],
                'volume': ['volume', 'Volume', 'volume_24h']
            }
            
            # Rename columns if they exist
            for target, variants in column_map.items():
                found = False
                for variant in variants:
                    if variant in df.columns:
                        df = df.rename(columns={variant: target})
                        found = True
                        break
                if not found and target != 'volume':  # volume might be optional
                    raise ValueError(f"Could not find {target} column in {file_path}")
            
            # Add token name and source file
            df['token_name'] = token_name
            df['source_file'] = os.path.basename(file_path)
            
            # Convert timestamp to datetime if it's not already
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Add volume as 0 if missing
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            # Insert into database
            df.to_sql('ohlcv', self.conn, if_exists='append', index=False)
            logger.info(f"Imported {len(df)} rows for {token_name}")
            
        except Exception as e:
            logger.error(f"Error importing {file_path}: {str(e)}")
    
    def process_directory(self, directory):
        """Process all CSV files in a directory"""
        try:
            # Find all CSV files in directory and subdirectories
            csv_files = glob.glob(os.path.join(directory, '**/*.csv'), recursive=True)
            
            for file_path in csv_files:
                logger.info(f"Processing {file_path}")
                self.import_csv_file(file_path)
            
        except Exception as e:
            logger.error(f"Error processing directory {directory}: {str(e)}")

def main():
    """Main function to consolidate OHLCV data"""
    consolidator = OHLCVConsolidator()
    
    try:
        # Connect and create tables
        consolidator.connect()
        consolidator.create_tables()
        
        # Process ohlcv_data directory
        ohlcv_dir = 'ohlcv_data'
        logger.info(f"Processing directory: {ohlcv_dir}")
        consolidator.process_directory(ohlcv_dir)
        
        logger.info("Data consolidation complete")
        
    finally:
        consolidator.close()

if __name__ == "__main__":
    main()
