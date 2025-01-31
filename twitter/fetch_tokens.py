import requests
import pandas as pd
import time

class JupiterTokenFetcher:
    def __init__(self):
        self.base_url = "https://token.jup.ag/all"

    def fetch_active_tokens(self):
        """
        Fetch active tokens from Jupiter API
        """
        try:
            response = requests.get(self.base_url)
            print("Response status:", response.status_code)
            
            if response.status_code == 200:
                tokens = response.json()
                
                # Convert to DataFrame
                df = pd.DataFrame(tokens)
                
                # Keep only relevant columns
                df = df[['address', 'symbol', 'name']].rename(columns={
                    'address': 'MintAddress',
                    'symbol': 'Symbol',
                    'name': 'Name'
                })
                
                # Save to CSV
                df.to_csv('active_tokens.csv', index=False)
                print(f"Saved {len(df)} tokens to active_tokens.csv")
                
                return df
            else:
                print(f"Error: {response.status_code}")
                print(response.text)
                return None
            
        except Exception as e:
            print(f"Error fetching tokens: {str(e)}")
            return None

def main():
    fetcher = JupiterTokenFetcher()
    tokens = fetcher.fetch_active_tokens()
    
    if tokens is not None:
        print("\nTop 1000 tokens found:")
        print(tokens.head(1000))

if __name__ == "__main__":
    main()
