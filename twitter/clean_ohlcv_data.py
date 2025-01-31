import os
import pandas as pd
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    data_dir = 'ohlcv_data'
    removed_count = 0
    kept_count = 0
    
    # Check if directory exists
    if not os.path.exists(data_dir):
        logger.error(f"Directory {data_dir} does not exist!")
        return
    
    # Get all CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    logger.info(f"Found {len(csv_files)} CSV files")
    
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Check if all numeric columns are zero
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            all_zeros = all((df[col] == 0).all() for col in numeric_cols)
            
            if all_zeros:
                os.remove(file_path)
                logger.info(f"Removed {file} - contained only zeros")
                removed_count += 1
            else:
                logger.info(f"Kept {file} - contains non-zero values")
                kept_count += 1
                
        except Exception as e:
            logger.error(f"Error processing {file}: {str(e)}")
    
    logger.info(f"Cleaning complete! Removed {removed_count} files, kept {kept_count} files")

if __name__ == "__main__":
    main()
