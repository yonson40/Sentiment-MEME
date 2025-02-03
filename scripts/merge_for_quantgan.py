import sqlite3
import os

# File paths
OHLCV_DB = os.path.join('databases', 'ohlcv.db')
SENTIMENT_DB = os.path.join('databases', 'sentiment_data.db')
UNIFIED_DB = os.path.join('databases', 'quantgan_training.db')


def create_unified_schema():
    conn = sqlite3.connect(UNIFIED_DB)
    cur = conn.cursor()
    
    # Create tokens table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            symbol TEXT PRIMARY KEY,
            created_at TEXT
        )
    ''')
    
    # Create market_features table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS market_features (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            FOREIGN KEY(token_symbol) REFERENCES tokens(symbol)
        )
    ''')
    
    # Create token_sentiment_timeseries table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS token_sentiment_timeseries (
            timestamp TEXT NOT NULL,
            token_symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            sentiment_mean REAL,
            sentiment_std REAL,
            tweet_count INTEGER,
            positive_ratio REAL,
            negative_ratio REAL,
            neutral_ratio REAL,
            engagement_score REAL,
            PRIMARY KEY (timestamp, token_symbol, interval),
            FOREIGN KEY(token_symbol) REFERENCES tokens(symbol)
        )
    ''')
    
    conn.commit()
    conn.close()


def migrate_tokens():
    # Migrate tokens from ohlcv.db
    src_conn = sqlite3.connect(OHLCV_DB)
    src_cur = src_conn.cursor()
    tokens = src_cur.execute("SELECT symbol, created_at FROM tokens").fetchall()
    src_conn.close()

    dst_conn = sqlite3.connect(UNIFIED_DB)
    dst_cur = dst_conn.cursor()
    
    for symbol, created_at in tokens:
        dst_cur.execute("INSERT OR IGNORE INTO tokens (symbol, created_at) VALUES (?, ?)", (symbol, created_at))
    
    dst_conn.commit()
    dst_conn.close()
    print(f"Migrated {len(tokens)} tokens.")


def migrate_market_features():
    # Migrate market data from ohlcv.db
    src_conn = sqlite3.connect(OHLCV_DB)
    src_cur = src_conn.cursor()

    query = """
        SELECT t.symbol, p.timestamp, p.open, p.high, p.low, p.close, p.volume
        FROM prices p
        JOIN tokens t ON p.token_id = t.id
    """
    rows = src_cur.execute(query).fetchall()
    src_conn.close()

    dst_conn = sqlite3.connect(UNIFIED_DB)
    dst_cur = dst_conn.cursor()
    
    for row in rows:
        symbol, ts, o, h, l, c, v = row
        dst_cur.execute("""
            INSERT INTO market_features (token_symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (symbol, ts, o, h, l, c, v))
    dst_conn.commit()
    dst_conn.close()
    print(f"Migrated {len(rows)} market feature records.")


def migrate_sentiment_timeseries():
    # Migrate sentiment data from sentiment_data.db
    src_conn = sqlite3.connect(SENTIMENT_DB)
    src_cur = src_conn.cursor()

    # Assumes table is named token_sentiment_timeseries and token column will be used as token_symbol
    query = "SELECT timestamp, token, interval, sentiment_mean, sentiment_std, tweet_count, positive_ratio, negative_ratio, neutral_ratio, engagement_score FROM token_sentiment_timeseries"
    rows = src_cur.execute(query).fetchall()
    src_conn.close()

    dst_conn = sqlite3.connect(UNIFIED_DB)
    dst_cur = dst_conn.cursor()
    
    for row in rows:
        # Replace token with token_symbol
        ts, token, interval, s_mean, s_std, tw_count, pos_ratio, neg_ratio, neu_ratio, eng_score = row
        dst_cur.execute("""
            INSERT INTO token_sentiment_timeseries
            (timestamp, token_symbol, interval, sentiment_mean, sentiment_std, tweet_count, positive_ratio, negative_ratio, neutral_ratio, engagement_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts, token, interval, s_mean, s_std, tw_count, pos_ratio, neg_ratio, neu_ratio, eng_score))
    dst_conn.commit()
    dst_conn.close()
    print(f"Migrated {len(rows)} sentiment timeseries records.")


def run_migration():
    print("Creating unified schema...")
    create_unified_schema()
    print("Migrating tokens...")
    migrate_tokens()
    print("Migrating market features...")
    migrate_market_features()
    print("Migrating sentiment timeseries data...")
    migrate_sentiment_timeseries()
    print("Migration complete. Unified database created at:", UNIFIED_DB)

if __name__ == '__main__':
    run_migration()
