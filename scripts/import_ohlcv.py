import os
import glob
import json
import pandas as pd
import sqlite3


def import_json_to_sqlite():
    # Path to the SQLite database
    db_path = os.path.join('databases', 'ohlcv.db')
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Create the ohlcv_data table if it doesn't exist
    cur.execute('''
    CREATE TABLE IF NOT EXISTS ohlcv_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT,
        datetime TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL
    )
    ''')

    # Directory containing the files
    data_folder = os.path.join(os.getcwd(), 'data', 'ohlcv')
    files = glob.glob(os.path.join(data_folder, '*.csv'))

    for file_path in files:
        # The token name is the filename before .csv
        token = os.path.basename(file_path).split('.')[0]
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                # Extract the trade data array
                trades = data['Solana']['DEXTradeByTokens']
                
                # Convert to DataFrame
                rows = []
                for trade in trades:
                    row = {
                        'token': token,
                        'datetime': trade['Block']['Time'],
                        'open': trade.get('open', trade['close']),  # fallback to close if open not available
                        'high': trade.get('max', trade['close']),   # max is used for high
                        'low': trade.get('min', trade['close']),    # min is used for low
                        'close': trade['close'],
                        'volume': trade.get('volume', 0)  # default to 0 if volume not available
                    }
                    rows.append(row)
                
                df = pd.DataFrame(rows)
                
                # Insert data into the SQLite database table
                df.to_sql('ohlcv_data', conn, if_exists='append', index=False)
                print(f"Imported data for token {token} from {file_path}")
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    conn.commit()
    conn.close()


if __name__ == '__main__':
    import_json_to_sqlite()
