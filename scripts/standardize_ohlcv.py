import pandas as pd
import json
import os
from datetime import datetime
import glob

def standardize_json_format(file_path):
    """Convert JSON formatted OHLCV data to standardized CSV format."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            # Handle both JSON files and JSON-like CSV files
            if content.strip().startswith('{'):
                data = json.loads(content)
                trades = data.get('Solana', {}).get('DEXTradeByTokens', [])
            else:
                # For files that look like JSON but are actually CSV
                df = pd.read_json(content, lines=True)
                trades = df.to_dict('records')
        
        records = []
        for trade in trades:
            # Handle nested Block.Time structure
            timestamp = trade.get('Block', {}).get('Time') if isinstance(trade.get('Block'), dict) else trade.get('Time')
            
            record = {
                'timestamp': pd.to_datetime(timestamp),
                'open': float(trade.get('open', trade.get('close', 0))),
                'high': float(trade.get('max', trade.get('high', trade.get('close', 0)))),
                'low': float(trade.get('min', trade.get('low', trade.get('close', 0)))),
                'close': float(trade.get('close', 0)),
                'volume': float(trade.get('volume', 0))
            }
            records.append(record)
        
        df = pd.DataFrame(records)
        df = df.sort_values('timestamp')
        return df
    except Exception as e:
        print(f"Error in JSON processing for {file_path}: {str(e)}")
        return None

def standardize_csv_format(file_path):
    """Standardize CSV formatted OHLCV data."""
    try:
        # Try reading as regular CSV first
        df = pd.read_csv(file_path)
        
        # If successful, standardize column names
        column_map = {
            'timestamp': 'timestamp',
            'time': 'timestamp',
            'Time': 'timestamp',
            'open': 'open',
            'Open': 'open',
            'high': 'high',
            'High': 'high',
            'max': 'high',
            'low': 'low',
            'Low': 'low',
            'min': 'low',
            'close': 'close',
            'Close': 'close',
            'volume': 'volume',
            'Volume': 'volume'
        }
        
        df = df.rename(columns=column_map)
        
        # Convert timestamp to datetime
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Ensure all required columns exist
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col not in df.columns:
                if col == 'volume':
                    df[col] = 0
                else:
                    df[col] = df['close']
        
        return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    except pd.errors.EmptyDataError:
        print(f"Empty file: {file_path}")
        return None
    except Exception as e:
        # If regular CSV reading fails, try JSON format
        try:
            return standardize_json_format(file_path)
        except Exception as json_e:
            print(f"Error processing {file_path}: {str(e)}, JSON error: {str(json_e)}")
            return None

def process_file(input_file, output_dir):
    """Process a single file and save it in standardized format."""
    try:
        # Determine if file is JSON-like or CSV
        with open(input_file, 'r') as f:
            content = f.read(1024)  # Read first 1KB to check format
            
        if content.strip().startswith('{') or '"Block":' in content:
            df = standardize_json_format(input_file)
        else:
            df = standardize_csv_format(input_file)
        
        if df is not None and not df.empty:
            # Sort by timestamp
            df = df.sort_values('timestamp')
            
            # Format timestamp to ISO 8601
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # Create output filename
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            if not base_name.endswith('_ohlcv'):
                base_name += '_ohlcv'
            
            output_file = os.path.join(output_dir, f"{base_name}.csv")
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            print(f"Successfully processed: {input_file} -> {output_file}")
        else:
            print(f"No valid data found in {input_file}")
            
    except Exception as e:
        print(f"Error processing {input_file}: {str(e)}")

def main():
    # Directory containing OHLCV files
    input_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ohlcv_data')
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ohlcv_data_standardized')
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Process all files in the directory
    for file_pattern in ['*.csv', '*.json']:
        for file_path in glob.glob(os.path.join(input_dir, '**', file_pattern), recursive=True):
            process_file(file_path, output_dir)

if __name__ == "__main__":
    main()
