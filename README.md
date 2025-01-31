# Sentiment Analysis for Crypto Meme Tokens

A comprehensive sentiment analysis system for analyzing social media sentiment and its correlation with cryptocurrency token prices, specifically focused on meme tokens.

## Project Structure

```
├── database/               # Database management scripts
├── models/                 # ML models including GAN implementation
├── ohlcv_data/            # Raw OHLCV data for various tokens
├── ohlcv_data_standardized/# Standardized OHLCV data
├── scripts/               # Core analysis scripts
└── twitter/               # Twitter data collection scripts
```

## Key Features

- Custom VADER sentiment analysis tuned for crypto and meme-specific terms
- Real-time token price data collection via DexRabbit
- Twitter data scraping and analysis
- Time series sentiment aggregation
- SQLite database for efficient data storage
- GAN model for synthetic data generation

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your API credentials:
   ```
   DEXRABBIT_EMAIL=your_email
   DEXRABBIT_PASSWORD=your_password
   ```

## Usage

1. Data Collection:
   ```python
   python twitter/data_collector.py  # Collect Twitter data
   python twitter/fetch_tokens.py    # Fetch token prices
   ```

2. Sentiment Analysis:
   ```python
   python scripts/meme_sentiment_analyzer.py
   ```

3. Data Standardization:
   ```python
   python scripts/standardize_ohlcv.py
   ```

## Database Schema

The project uses a single SQLite database (`sentiment_data.db`) containing:
- Tweet content and metadata
- Author information
- VADER sentiment scores
- Token mappings
- Sentiment time series data

## Contributing

Feel free to submit issues and enhancement requests!
