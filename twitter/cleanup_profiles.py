import os
import shutil

def cleanup_profile_data():
    profile_data_dir = 'profile_data'
    
    # Check if directory exists
    if not os.path.exists(profile_data_dir):
        print("profile_data directory not found")
        return
        
    # Get all subdirectories
    subdirs = [d for d in os.listdir(profile_data_dir) if os.path.isdir(os.path.join(profile_data_dir, d))]
    
    deleted_count = 0
    kept_count = 0
    
    for subdir in subdirs:
        dir_path = os.path.join(profile_data_dir, subdir)
        tweets_file = os.path.join(dir_path, 'tweets.csv')
        
        # If no tweets.csv, delete the directory
        if not os.path.exists(tweets_file):
            print(f"Deleting {dir_path} (no tweets.csv found)")
            shutil.rmtree(dir_path)
            deleted_count += 1
        else:
            kept_count += 1
            
    print(f"\nCleanup complete:")
    print(f"- Deleted {deleted_count} directories without tweets.csv")
    print(f"- Kept {kept_count} directories with tweets.csv")

if __name__ == "__main__":
    cleanup_profile_data()
