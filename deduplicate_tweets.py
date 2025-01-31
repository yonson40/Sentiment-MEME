import pandas as pd
import glob
import os
from datetime import datetime

# Get all following_tweets CSV files
csv_files = glob.glob('following_tweets_*.csv')

# Read and combine all CSVs
all_tweets = []
for file in csv_files:
    df = pd.read_csv(file)
    all_tweets.append(df)
    print(f"Read {len(df)} tweets from {file}")

# Combine all dataframes
combined_df = pd.concat(all_tweets, ignore_index=True)
print(f"\nTotal tweets before deduplication: {len(combined_df)}")

# Remove duplicates based on tweet_id
deduped_df = combined_df.drop_duplicates(subset=['tweet_id'], keep='first')
print(f"Total tweets after deduplication: {len(deduped_df)}")
print(f"Removed {len(combined_df) - len(deduped_df)} duplicate tweets")

# Save deduplicated data
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = f'following_tweets_deduped_{timestamp}.csv'
deduped_df.to_csv(output_file, index=False)
print(f"\nSaved deduplicated tweets to {output_file}")

# Delete original files
for file in csv_files:
    os.remove(file)
    print(f"Deleted {file}")
