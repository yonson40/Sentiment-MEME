import sqlite3
import os
from datetime import datetime

# Configuration
GAN_DB_PATH = os.path.join('databases', 'quantgan_training.db')

# Create optimized GAN-ready schema
def create_gan_schema():
    conn = sqlite3.connect(GAN_DB_PATH)
    cur = conn.cursor()
    
    # Core temporal table with unified timeline
    cur.execute('''
    CREATE TABLE temporal_units (
        timestamp INTEGER PRIMARY KEY,  -- Unix timestamp
        time_str TEXT NOT NULL,         -- ISO 8601
        time_interval TEXT NOT NULL     -- 1m, 5m, 15m, 1h
    )
    ''')

    # Asset fundamentals
    cur.execute('''
    CREATE TABLE assets (
        symbol TEXT PRIMARY KEY,
        created_at INTEGER,
        first_price REAL,
        last_price REAL,
        volatility_24h REAL
    )
    ''')

    # Unified market/sentiment features
    cur.execute('''
    CREATE TABLE market_features (
        timestamp INTEGER,
        symbol TEXT,
        -- OHLCV Features
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        returns_5m REAL,
        spread REAL,
        -- Sentiment Features
        sentiment_mean REAL,
        sentiment_volatility REAL,
        tweet_volume INTEGER,
        engagement_score REAL,
        -- Technical Indicators
        rsi_14 REAL,
        macd REAL,
        bollinger_upper REAL,
        bollinger_lower REAL,
        
        PRIMARY KEY (timestamp, symbol),
        FOREIGN KEY(timestamp) REFERENCES temporal_units(timestamp),
        FOREIGN KEY(symbol) REFERENCES assets(symbol)
    )
    ''')

    # Create indexes for GAN training
    cur.execute('CREATE INDEX idx_features ON market_features (timestamp, symbol)')
    cur.execute('CREATE INDEX idx_assets ON assets (symbol, created_at)')
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_gan_schema()
    print(f"Created optimized GAN database at {GAN_DB_PATH}")
