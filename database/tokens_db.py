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

class TokensDatabase:
    def __init__(self, db_path='minute_data/alien_ohlcv/TOKENS.db'):
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
    
    def get_table_data(self, table_name, limit=5):
        """Get data from a table with optional limit"""
        try:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql_query(query, self.conn)
            logger.info(f"Retrieved {len(df)} rows from {table_name}")
            return df
        except Exception as e:
            logger.error(f"Error getting data from {table_name}: {str(e)}")
            return pd.DataFrame()

def main():
    """Example usage"""
    db = TokensDatabase()
    try:
        # Connect to database
        db.connect()
        
        # Get tables
        print("\nTables in database:")
        tables = db.get_tables()
        for table in tables:
            print(f"\n- {table}")
            
            # Get schema
            columns = db.get_table_schema(table)
            print(f"  Columns: {columns}")
            
            # Get sample data
            print(f"\n  Sample data:")
            df = db.get_table_data(table, limit=5)
            print(df)
            print("\n" + "="*80)
        
    finally:
        db.close()

if __name__ == "__main__":
    main()
