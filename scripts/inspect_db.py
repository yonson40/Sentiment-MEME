import sqlite3

def inspect_db(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # List tables
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"Tables in {db_path}:")
    for table in tables:
        print(f"- {table[0]}")
        # Get columns
        cols = cur.execute(f"PRAGMA table_info({table[0]})").fetchall()
        print("  Columns:", ", ".join([col[1] for col in cols]))
    
    conn.close()

# Inspect both databases
if __name__ == '__main__':
    inspect_db('databases/sentiment_data.db')
    inspect_db('databases/ohlcv.db')
