import pandas as pd
import sqlite3
import os
from pathlib import Path

def load_ohlcv_data(ohlcv_dir):
    """Load and combine all standardized OHLCV data"""
    all_data = []
    
    for file in os.listdir(ohlcv_dir):
        if file.endswith('_ohlcv.csv'):
            token_name = file.replace('_ohlcv.csv', '')
            df = pd.read_csv(os.path.join(ohlcv_dir, file))
            df['token'] = token_name
            all_data.append(df)
    
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
    return combined_df

def insert_ohlcv_to_db():
    # Define paths
    base_dir = Path(__file__).parent.parent
    tokens_db_path = base_dir / 'minute_data/alien_ohlcv/TOKENS.db'
    ohlcv_dir = base_dir / 'ohlcv_data_standardized'
    
    # Load OHLCV data
    print("Loading OHLCV data...")
    ohlcv_df = load_ohlcv_data(ohlcv_dir)
    
    # Connect to database
    print("Connecting to database...")
    conn = sqlite3.connect(tokens_db_path)
    cursor = conn.cursor()
    
    # Drop existing table if it exists
    print("Dropping existing OHLCV table if it exists...")
    cursor.execute("DROP TABLE IF EXISTS ohlcv")
    
    # Create OHLCV table
    print("Creating OHLCV table...")
    cursor.execute("""
    CREATE TABLE ohlcv (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        token TEXT NOT NULL,
        UNIQUE(timestamp, token)
    )
    """)
    
    # Insert data in batches
    print("Inserting data into database...")
    batch_size = 1000
    total_rows = len(ohlcv_df)
    
    for i in range(0, total_rows, batch_size):
        batch = ohlcv_df.iloc[i:i+batch_size]
        try:
            batch.to_sql('ohlcv', conn, if_exists='append', index=False)
            print(f"Processed {min(i+batch_size, total_rows)}/{total_rows} rows...")
        except sqlite3.IntegrityError:
            print(f"Skipping duplicate entries in batch {i//batch_size + 1}")
            continue
    
    # Create indices for better performance
    print("Creating indices...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_token ON ohlcv(token)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_timestamp ON ohlcv(timestamp)")
    
    # Remove any remaining duplicates (although UNIQUE constraint should prevent them)
    print("Removing any remaining duplicates...")
    cursor.execute("""
    DELETE FROM ohlcv 
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM ohlcv
        GROUP BY timestamp, token
    )
    """)
    
    # Print statistics
    cursor.execute("SELECT COUNT(DISTINCT token) as token_count FROM ohlcv")
    token_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) as total_rows FROM ohlcv")
    total_rows = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date FROM ohlcv")
    min_date, max_date = cursor.fetchone()
    
    print("\nDatabase Statistics:")
    print(f"Total number of tokens: {token_count}")
    print(f"Total number of price points: {total_rows}")
    print(f"Date range: {min_date} to {max_date}")
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print("\nDone! All OHLCV data has been inserted into TOKENS.db with duplicates removed.")

if __name__ == "__main__":
    insert_ohlcv_to_db()
