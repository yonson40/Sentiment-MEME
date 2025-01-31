import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
api_url = "https://graphql.bitquery.io/"
api_key = os.getenv('API_2')
api_token = os.getenv('TOKEN_2')

if not api_key or not api_token:
    raise Exception("API_2 or TOKEN_2 not found in .env file")

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_token}"
}

# Calculate date range for last 24 hours
end_date = datetime.now()
start_date = end_date - timedelta(hours=24)

query = """
query($start: ISO8601DateTime!, $end: ISO8601DateTime!) {
  solana(network: solana) {
    transactions(
      options: {limit: 50, desc: "Block_Time"}
      date: {since: $start, till: $end}
      success: {is: true}
    ) {
      block {
        timestamp {
          time
        }
      }
      success
      transactionFee
      signature
      feePayer
      accountsCount
    }
  }
}
"""

variables = {
    "start": start_date.isoformat(),
    "end": end_date.isoformat()
}

try:
    # Make the request
    response = requests.post(
        api_url,
        json={'query': query, 'variables': variables},
        headers=headers
    )
    
    # Check if request was successful
    response.raise_for_status()
    
    # Parse the response
    data = response.json()
    print("Response:", json.dumps(data, indent=2))
    
    if 'errors' in data:
        print("GraphQL Errors:")
        for error in data['errors']:
            print(f"- {error.get('message', 'Unknown error')}")
        exit(1)
        
    if not data.get('data', {}).get('solana', {}).get('transactions'):
        print("No transaction data found in response")
        exit(1)
    
    # Process the data
    transactions = data['data']['solana']['transactions']
    processed_txs = []
    
    for tx in transactions:
        processed_tx = {
            'timestamp': tx['block']['timestamp']['time'],
            'success': tx['success'],
            'fee': tx['transactionFee'],
            'signature': tx['signature'],
            'fee_payer': tx['feePayer'],
            'accounts_count': tx['accountsCount']
        }
        processed_txs.append(processed_tx)
    
    # Save to JSON file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'transaction_data_{timestamp}.json'
    
    with open(output_file, 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'total_transactions': len(processed_txs),
            'transactions': processed_txs
        }, f, indent=2)
    
    print(f"Successfully fetched {len(processed_txs)} transactions")
    print(f"Data saved to {output_file}")
    
    # Print some sample data
    print("\nSample transactions:")
    for tx in processed_txs[:5]:
        print(f"Time: {tx['timestamp']}")
        print(f"Success: {tx['success']}")
        print(f"Fee: {tx['fee']}")
        print(f"Signature: {tx['signature']}")
        print(f"Fee Payer: {tx['fee_payer']}")
        print(f"Accounts Count: {tx['accounts_count']}")
        print("---")

except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print(f"Status code: {e.response.status_code}")
        print(f"Response body: {e.response.text}")
except Exception as e:
    print(f"An error occurred: {e}")