import requests
import pandas as pd
import time
from datetime import datetime
from pathlib import Path
import re

# Configuration
OUTPUT_FOLDER = "Kalshi_market_data"
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
CANDLESTICK_LIMIT = 4900  # Stay under 5000 limit with buffer

# Market URLs to fetch - ADD YOUR URLS HERE
MARKET_URLS = [
    "https://kalshi.com/markets/kxbtcvsgold/btc-vs-gold/kxbtcvsgold-25",
]


def extract_ticker_from_url(url):
    """Extract the ticker from a Kalshi URL."""
    match = re.search(r'/([^/]+)$', url)
    return match.group(1).upper() if match else None


def extract_event_ticker_from_url(url):
    """Extract the event ticker from a Kalshi URL (second to last part)."""
    parts = url.rstrip('/').split('/')
    if len(parts) >= 2:
        return parts[-1].upper()
    return None


def fetch_market_data(ticker):
    """Fetch market data for a specific ticker."""
    url = f"{BASE_URL}/markets/{ticker}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching market {ticker}: {e}")
        return None


def fetch_event_data(event_ticker):
    """Fetch event data and extract all markets."""
    url = f"{BASE_URL}/events/{event_ticker}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get('markets', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching event {event_ticker}: {e}")
        return []


def fetch_candlesticks(series_ticker, market_ticker, start_ts, end_ts):
    """Fetch candlestick data for a specific time range."""
    url = f"{BASE_URL}/series/{series_ticker}/markets/{market_ticker}/candlesticks"
    params = {
        'start_ts': start_ts,
        'end_ts': end_ts,
        'period_interval': 1  # 1 minute intervals
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('candlesticks', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching candlesticks: {e}")
        return []


def get_all_candlesticks(series_ticker, market_ticker, start_ts, end_ts):
    """Fetch all candlesticks, handling the 5000 limit by making multiple requests."""
    all_candlesticks = []
    current_start = start_ts
    
    # Each candlestick is 1 minute (60 seconds)
    chunk_duration = CANDLESTICK_LIMIT * 60
    
    while current_start < end_ts:
        current_end = min(current_start + chunk_duration, end_ts)
        
        print(f"  Fetching data from {datetime.fromtimestamp(current_start)} to {datetime.fromtimestamp(current_end)}")
        
        candlesticks = fetch_candlesticks(series_ticker, market_ticker, current_start, current_end)
        
        if candlesticks:
            all_candlesticks.extend(candlesticks)
            print(f"  Retrieved {len(candlesticks)} candlesticks")
        
        current_start = current_end
        
        # Small delay to avoid rate limiting
        time.sleep(0.5)
    
    return all_candlesticks


def extract_series_ticker(event_ticker):
    """Extract series ticker from event ticker (remove date suffix)."""
    # Series ticker is typically the event ticker without the date part
    match = re.match(r'^([A-Z]+)', event_ticker)
    return match.group(1) if match else event_ticker


def process_candlesticks_to_dataframe(candlesticks):
    """Convert candlesticks data to a pandas DataFrame."""
    rows = []
    
    for candle in candlesticks:
        timestamp = candle.get('end_period_ts')
        dt = datetime.fromtimestamp(timestamp) if timestamp else None
        
        row = {
            'timestamp': timestamp,
            'datetime': dt,
            'open_interest': candle.get('open_interest'),
            'volume': candle.get('volume'),
        }
        
        # Add price data
        price = candle.get('price', {})
        for key in ['open', 'close', 'high', 'low', 'mean']:
            row[f'price_{key}'] = price.get(key)
            row[f'price_{key}_dollars'] = price.get(f'{key}_dollars')
        
        # Add yes_ask data
        yes_ask = candle.get('yes_ask', {})
        for key in ['open', 'close', 'high', 'low']:
            row[f'yes_ask_{key}'] = yes_ask.get(key)
            row[f'yes_ask_{key}_dollars'] = yes_ask.get(f'{key}_dollars')
        
        # Add yes_bid data
        yes_bid = candle.get('yes_bid', {})
        for key in ['open', 'close', 'high', 'low']:
            row[f'yes_bid_{key}'] = yes_bid.get(key)
            row[f'yes_bid_{key}_dollars'] = yes_bid.get(f'{key}_dollars')
        
        rows.append(row)
    
    return pd.DataFrame(rows)


def save_to_csv(df, ticker, output_folder):
    """Save DataFrame to CSV file."""
    Path(output_folder).mkdir(exist_ok=True)
    
    # Save full dataset
    full_filename = f"{output_folder}/{ticker}_full.csv"
    df.to_csv(full_filename, index=False)
    print(f"✓ Saved {len(df)} rows (full data) to {full_filename}")
    
    # Save filtered dataset with only rows that have price data (trades occurred)
    df_trades = df[df['price_close'].notna()].copy()
    if len(df_trades) > 0:
        trades_filename = f"{output_folder}/{ticker}_trades.csv"
        df_trades.to_csv(trades_filename, index=False)
        print(f"✓ Saved {len(df_trades)} rows (trades only) to {trades_filename}")
    else:
        print(f"  No trades found in the data for {ticker}")


def process_market(market_data):
    """Process a single market and fetch its historical data."""
    ticker = market_data.get('ticker')
    event_ticker = market_data.get('event_ticker')
    open_time = market_data.get('open_time')
    
    if not ticker or not event_ticker or not open_time:
        print(f"Missing required fields for market")
        return
    
    # Parse open_time to timestamp
    open_dt = datetime.fromisoformat(open_time.replace('Z', '+00:00'))
    start_ts = int(open_dt.timestamp())
    end_ts = int(datetime.now().timestamp())
    
    series_ticker = extract_series_ticker(event_ticker)
    
    print(f"\nProcessing market: {ticker}")
    print(f"  Series: {series_ticker}")
    print(f"  Event: {event_ticker}")
    print(f"  Start date: {open_dt}")
    print(f"  End date: {datetime.now()}")
    
    # Fetch all candlesticks
    candlesticks = get_all_candlesticks(series_ticker, ticker, start_ts, end_ts)
    
    if not candlesticks:
        print(f"  No candlestick data found for {ticker}")
        return
    
    # Convert to DataFrame and save
    df = process_candlesticks_to_dataframe(candlesticks)
    save_to_csv(df, ticker, OUTPUT_FOLDER)


def main():
    """Main function to process all market URLs."""
    print(f"Starting Kalshi data fetch at {datetime.now()}")
    print(f"Output folder: {OUTPUT_FOLDER}\n")
    
    markets_to_process = []
    
    for url in MARKET_URLS:
        print(f"Processing URL: {url}")
        ticker = extract_ticker_from_url(url)
        
        if not ticker:
            print(f"  Could not extract ticker from URL")
            continue
        
        # Try as market first
        market_data = fetch_market_data(ticker)
        
        if market_data and 'market' in market_data:
            print(f"  ✓ Found as market: {ticker}")
            markets_to_process.append(market_data['market'])
        else:
            # Try as event
            print(f"  Not a market, trying as event...")
            event_ticker = extract_event_ticker_from_url(url)
            markets = fetch_event_data(event_ticker)
            
            if markets:
                print(f"  ✓ Found as event with {len(markets)} markets")
                markets_to_process.extend(markets)
            else:
                print(f"  ✗ Could not fetch data for {ticker}")
    
    # Process all markets
    print(f"\n{'='*60}")
    print(f"Found {len(markets_to_process)} market(s) to process")
    print(f"{'='*60}")
    
    for market_data in markets_to_process:
        try:
            process_market(market_data)
        except Exception as e:
            print(f"Error processing market {market_data.get('ticker')}: {e}")
    
    print(f"\n{'='*60}")
    print(f"Completed at {datetime.now()}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()