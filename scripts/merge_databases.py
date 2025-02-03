import sqlite3
import os
from tqdm import tqdm

# Configuration
OLD_SENTIMENT_DB = os.path.join('databases', 'sentiment_data.db')
OLD_OHLCV_DB = os.path.join('databases', 'ohlcv.db')
NEW_DB = os.path.join('databases', 'sentiment_meme.db')

# Create new database with unified schema
def create_unified_schema():
    conn = sqlite3.connect(NEW_DB)
    cur = conn.cursor()
    
    # Create tables from ohlcv.db
    cur.execute('''
    CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id INTEGER NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        open DECIMAL(18,8) NOT NULL,
        high DECIMAL(18,8) NOT NULL,
        low DECIMAL(18,8) NOT NULL,
        close DECIMAL(18,8) NOT NULL,
        volume DECIMAL(18,8) NOT NULL,
        FOREIGN KEY(token_id) REFERENCES tokens(id)
    )
    ''')

    # Create tables from sentiment_data.db
    cur.execute('''
    CREATE TABLE IF NOT EXISTS token_sentiment_timeseries (
        timestamp TIMESTAMP NOT NULL,
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        sentiment_mean DECIMAL(5,4) NOT NULL,
        sentiment_std DECIMAL(5,4) NOT NULL,
        tweet_count INTEGER NOT NULL,
        positive_ratio DECIMAL(5,4),
        negative_ratio DECIMAL(5,4),
        neutral_ratio DECIMAL(5,4),
        engagement_score DECIMAL(10,2),
        FOREIGN KEY(symbol) REFERENCES tokens(symbol)
    )
    ''')

    conn.commit()
    conn.close()

# Migrate data with progress bars
def migrate_data():
    # Connect to all databases
    src_conn = sqlite3.connect(OLD_OHLCV_DB)
    dst_conn = sqlite3.connect(NEW_DB)
    sentiment_conn = sqlite3.connect(OLD_SENTIMENT_DB)
    
    # Migrate tokens
    print("Migrating tokens...")
    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()
    
    tokens = src_cur.execute("SELECT symbol, created_at FROM tokens").fetchall()
    for symbol, created_at in tqdm(tokens):
        dst_cur.execute("INSERT OR IGNORE INTO tokens (symbol, created_at) VALUES (?, ?)", 
                      (symbol, created_at))
    
    # Migrate prices
    print("\nMigrating price data...")
    src_cur.execute("""
        SELECT t.symbol, p.timestamp, p.open, p.high, p.low, p.close, p.volume
        FROM prices p
        JOIN tokens t ON p.token_id = t.id
    """)
    
    prices = src_cur.fetchall()
    for symbol, ts, o, h, l, c, v in tqdm(prices):
        dst_cur.execute("""
            INSERT INTO prices (token_id, timestamp, open, high, low, close, volume)
            VALUES (
                (SELECT id FROM tokens WHERE symbol = ?),
                ?, ?, ?, ?, ?, ?
            )
        """, (symbol, ts, o, h, l, c, v))
    
    # Migrate sentiment timeseries
    print("\nMigrating sentiment data...")
    sentiment_cur = sentiment_conn.cursor()
    sentiment_data = sentiment_cur.execute("""
        SELECT timestamp, token, interval, sentiment_mean, sentiment_std,
               tweet_count, positive_ratio, negative_ratio, neutral_ratio, engagement_score
        FROM token_sentiment_timeseries
    """).fetchall()
    
    for row in tqdm(sentiment_data):
        dst_cur.execute("""
            INSERT INTO token_sentiment_timeseries
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)
    
    dst_conn.commit()
    src_conn.close()
    sentiment_conn.close()
    dst_conn.close()

if __name__ == '__main__':
    create_unified_schema()
    migrate_data()
    print("\nDatabase merge complete! New database:", NEW_DB)
