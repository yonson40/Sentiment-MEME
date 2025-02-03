import sqlite3
import os

def compare_and_merge_dbs():
    # Connect to both databases
    ohlcv_path = os.path.join('databases', 'ohlcv.db')
    tokens_path = os.path.join('databases', 'token_data.sqlite')
    
    if not os.path.exists(tokens_path):
        print("tokens.db doesn't exist")
        return
        
    ohlcv_conn = sqlite3.connect(ohlcv_path)
    tokens_conn = sqlite3.connect(tokens_path)
    
    # Get data from both databases
    try:
        tokens_df = tokens_conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        print("Tables in tokens.db:", tokens_df)
        
        if len(tokens_df) > 0:
            for table in tokens_df:
                table_name = table[0]
                data = tokens_conn.execute(f"SELECT * FROM {table_name}").fetchall()
                print(f"Table {table_name} has {len(data)} rows")
                
                # Get column names
                columns = tokens_conn.execute(f"PRAGMA table_info({table_name})").fetchall()
                print(f"Columns in {table_name}:", [col[1] for col in columns])
                
                # Compare with ohlcv.db structure
                ohlcv_tables = ohlcv_conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
                print("Tables in ohlcv.db:", ohlcv_tables)
                
                if len(ohlcv_tables) > 0:
                    for ohlcv_table in ohlcv_tables:
                        ohlcv_cols = ohlcv_conn.execute(f"PRAGMA table_info({ohlcv_table[0]})").fetchall()
                        print(f"Columns in {ohlcv_table[0]}:", [col[1] for col in ohlcv_cols])
        
        else:
            print("tokens.db is empty")
            
    except sqlite3.Error as e:
        print(f"Error reading from databases: {e}")
    finally:
        tokens_conn.close()
        ohlcv_conn.close()

if __name__ == '__main__':
    compare_and_merge_dbs()
