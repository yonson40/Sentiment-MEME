import os
import pandas as pd
import sqlite3
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_database():
    """Create SQLite database and tables"""
    conn = sqlite3.connect('ohlcv.db')
    cursor = conn.cursor()
    
    # Create tokens table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create prices table with foreign key to tokens
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id INTEGER,
        timestamp TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        FOREIGN KEY (token_id) REFERENCES tokens(id),
        UNIQUE(token_id, timestamp)
    )
    ''')
    
    conn.commit()
    return conn

def process_csv_files(conn):
    """Process all CSV files in the ohlcv_data directory"""
    data_dir = 'ohlcv_data'
    cursor = conn.cursor()
    
    # Get list of CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_SOL_ohlcv.csv')]
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    for file in csv_files:
        try:
            # Extract symbol from filename (remove _SOL_ohlcv.csv)
            symbol = file.replace('_SOL_ohlcv.csv', '')
            
            # Insert token if it doesn't exist
            cursor.execute('INSERT OR IGNORE INTO tokens (symbol) VALUES (?)', (symbol,))
            conn.commit()
            
            # Get token_id
            cursor.execute('SELECT id FROM tokens WHERE symbol = ?', (symbol,))
            token_id = cursor.fetchone()[0]
            
            # Read CSV file
            df = pd.read_csv(os.path.join(data_dir, file))
            logger.info(f"Processing {symbol}: {len(df)} rows")
            
            # Convert timestamp to datetime and then to string format
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert price data
            for _, row in df.iterrows():
                cursor.execute('''
                INSERT OR REPLACE INTO prices 
                (token_id, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    token_id,
                    row['timestamp'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume']
                ))
            
            conn.commit()
            logger.info(f"Successfully processed {symbol}")
            
        except Exception as e:
            logger.error(f"Error processing {file}: {str(e)}")
            continue

def verify_data(conn):
    """Verify the data was imported correctly"""
    cursor = conn.cursor()
    
    # Get total number of tokens
    cursor.execute('SELECT COUNT(*) FROM tokens')
    token_count = cursor.fetchone()[0]
    
    # Get total number of price records
    cursor.execute('SELECT COUNT(*) FROM prices')
    price_count = cursor.fetchone()[0]
    
    # Get sample of data
    cursor.execute('''
    SELECT t.symbol, p.timestamp, p.open, p.close, p.volume 
    FROM prices p 
    JOIN tokens t ON p.token_id = t.id 
    ORDER BY p.timestamp DESC 
    LIMIT 5
    ''')
    sample_data = cursor.fetchall()
    
    logger.info(f"Database summary:")
    logger.info(f"Total tokens: {token_count}")
    logger.info(f"Total price records: {price_count}")
    logger.info("Sample of recent price data:")
    for row in sample_data:
        logger.info(f"{row[0]}: {row[1]} - Open: {row[2]}, Close: {row[3]}, Volume: {row[4]}")

def main():
    try:
        # Create database and tables
        logger.info("Creating database...")
        conn = create_database()
        
        # Process CSV files
        logger.info("Processing CSV files...")
        process_csv_files(conn)
        
        # Verify data
        logger.info("Verifying data...")
        verify_data(conn)
        
        conn.close()
        logger.info("Database creation complete!")
        
    except Exception as e:
        logger.error(f"Error creating database: {str(e)}")

if __name__ == "__main__":
    main()
