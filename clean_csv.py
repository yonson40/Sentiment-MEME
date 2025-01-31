import pandas as pd

def clean_csv():
    # Create lists to store MintAddress and Symbol
    mint_addresses = set()
    symbols = set()
    
    # Read the file line by line
    with open('qq.csv', 'r') as file:
        for line in file:
            if '"MintAddress":' in line:
                # Extract MintAddress
                mint_address = line.split('"MintAddress": "')[1].split('"')[0].strip()
                mint_addresses.add(mint_address)
            elif '"Symbol":' in line:
                # Extract Symbol
                symbol = line.split('"Symbol": "')[1].split('"')[0].strip()
                symbols.add(symbol)
    
    # Create lists of equal length
    final_mint_addresses = []
    final_symbols = []
    
    # Match mint addresses with symbols (assuming they appear in pairs)
    mint_addresses = sorted(list(mint_addresses))
    symbols = sorted(list(symbols))
    
    # Take the shorter length to ensure we have pairs
    length = min(len(mint_addresses), len(symbols))
    
    for i in range(length):
        final_mint_addresses.append(mint_addresses[i])
        final_symbols.append(symbols[i])
    
    # Create a DataFrame
    df = pd.DataFrame({
        'MintAddress': final_mint_addresses,
        'Symbol': final_symbols
    })
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Remove rows where either MintAddress or Symbol is empty
    df = df.dropna()
    df = df[df['MintAddress'] != '']
    df = df[df['Symbol'] != '']
    
    # Save to new CSV
    output_file = 'cleaned_tokens.csv'
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")
    print(f"Total unique tokens: {len(df)}")
    
    # Print first few rows
    print("\nFirst few rows of the cleaned data:")
    print(df.head())

if __name__ == "__main__":
    clean_csv()
