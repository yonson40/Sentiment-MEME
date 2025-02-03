import sqlite3
from pprint import pprint

def analyze_database(db_path):
    print(f'\n=== Analyzing {db_path} ===')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get list of tables
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    
    for table in tables:
        table_name = table[0]
        print(f'\nTable: {table_name}')
        
        # Get column info
        columns = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        print('Columns:')
        pprint([col[1] for col in columns])
        
        # Get sample data
        try:
            sample = cursor.execute(f"SELECT * FROM {table_name} LIMIT 1").fetchone()
            print('\nSample row:')
            pprint(dict(zip([col[1] for col in columns], sample)))
        except Exception as e:
            print(f'Could not retrieve sample: {e}')
    
    conn.close()

if __name__ == '__main__':
    print('Analyzing sentiment_data.db...')
    analyze_database('databases/sentiment_data.db')
    
    print('\nAnalyzing ohlcv.db...')
    analyze_database('databases/ohlcv.db')
