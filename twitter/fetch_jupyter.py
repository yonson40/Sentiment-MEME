import requests
import pandas as pd

def fetch_active_tokens(limit=15000):
    """
    Fetch active tokens from Jupiter API with only address, symbol, and name
    """
    try:
        response = requests.get("https://token.jup.ag/all")
        if response.status_code == 200:
            tokens = response.json()
            df = pd.DataFrame(tokens)[['address', 'symbol', 'name']]
            
            # Limit to specified number of tokens
            df = df.head(limit)
            
            df.to_csv('jupiter.csv', index=False)
            print(f"Saved {len(df)} tokens to jupiter.csv")
            return df
        else:
            print(f"Error: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching tokens: {str(e)}")
        return None

if __name__ == "__main__":
    fetch_active_tokens(15000)