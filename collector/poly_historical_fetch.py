import requests
import csv
from datetime import datetime, timezone, timedelta
import time
import json

# ============ CONFIGURATION ============
# Input CSV file with market info
input_csv = 'polymarket_tokens.csv'

# Data resolution (1 = minute data, 60 = hourly data)
fidelity = 1

# Chunk size in days (7 days works well for minute data)
chunk_days = 7

# Output directory for CSV files
output_dir = 'market_data'
# ========================================

import os
os.makedirs(output_dir, exist_ok=True)

def get_market_start_date(slug):
    """Get market start date directly from slug using Gamma API"""
    try:
        gamma_slug_url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
        response = requests.get(gamma_slug_url)
        
        if response.status_code == 200:
            market = response.json()
            if 'startDate' in market and market['startDate']:
                start_date = datetime.fromisoformat(market['startDate'].replace('Z', '+00:00'))
                return start_date
    except Exception as e:
        print(f"    Warning: Could not get start date from slug: {e}")
    
    return None

def fetch_market_data(token_id, slug, start_date=None):
    """Fetch historical data for a single token"""
    
    print(f"\n{'='*80}")
    print(f"Processing: {slug}")
    print(f"Token ID: {token_id}")
    print(f"{'='*80}")
    
    # Try to get start date from slug
    if start_date is None:
        print("Fetching market start date...")
        start_date = get_market_start_date(slug)
        
        if start_date:
            print(f"Market start date: {start_date}")
        else:
            print("Could not determine start date, using fallback...")
            start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    # Verify earliest available data
    print("Verifying earliest available data...")
    test_url = 'https://clob.polymarket.com/prices-history'
    
    test_params = {
        'market': token_id,
        'startTs': int(start_date.timestamp()),
        'endTs': int((start_date + timedelta(days=1)).timestamp()),
        'fidelity': fidelity
    }
    test_response = requests.get(test_url, params=test_params)
    
    if test_response.status_code == 200:
        test_data = test_response.json()
        if 'history' in test_data and len(test_data['history']) > 0:
            earliest_found = test_data['history'][0]['t']
            start_date = datetime.fromtimestamp(earliest_found, tz=timezone.utc)
            print(f"Earliest data found at: {start_date}")
        else:
            print("No data at start date, searching forward...")
            search_date = start_date
            end_date_limit = datetime.now(timezone.utc)
            found = False
            
            while search_date < end_date_limit:
                test_params = {
                    'market': token_id,
                    'startTs': int(search_date.timestamp()),
                    'endTs': int((search_date + timedelta(days=30)).timestamp()),
                    'fidelity': fidelity
                }
                test_response = requests.get(test_url, params=test_params)
                if test_response.status_code == 200:
                    test_data = test_response.json()
                    if 'history' in test_data and len(test_data['history']) > 0:
                        earliest_found = test_data['history'][0]['t']
                        start_date = datetime.fromtimestamp(earliest_found, tz=timezone.utc)
                        print(f"Earliest data found at: {start_date}")
                        found = True
                        break
                search_date += timedelta(days=30)
                time.sleep(0.2)
            
            if not found:
                print("No historical data available for this market!")
                return None
    
    # Set end date to now
    end_date = datetime.now(timezone.utc)
    
    print(f"Fetching data from {start_date} to {end_date}")
    print(f"Duration: {(end_date - start_date).days} days")
    
    # Define the API endpoint
    url = 'https://clob.polymarket.com/prices-history'
    
    # Store all data
    all_data = []
    
    # Fetch data in chunks
    current_start = start_date
    chunk_count = 0
    local_chunk_days = chunk_days
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=local_chunk_days), end_date)
        
        start_ts = int(current_start.timestamp())
        end_ts = int(current_end.timestamp())
        
        chunk_count += 1
        print(f"Chunk {chunk_count}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}...", end=' ')
        
        params = {
            'market': token_id,
            'startTs': start_ts,
            'endTs': end_ts,
            'fidelity': fidelity
        }
        
        # Fetch the data
        response = requests.get(url, params=params)
        
        # Check response status
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            
            # If interval too long, try smaller chunk
            if "interval is too long" in response.text and local_chunk_days > 1:
                print(f"  Retrying with smaller chunk size...")
                local_chunk_days = max(1, local_chunk_days // 2)
                continue
            
            # Move to next chunk on other errors
            current_start = current_end
            continue
        
        data = response.json()
        
        # Add data from this chunk
        if 'history' in data and len(data['history']) > 0:
            all_data.extend(data['history'])
            print(f"{len(data['history'])} records")
        else:
            print("No data")
        
        # Move to next chunk
        current_start = current_end
        
        # Small delay to avoid rate limiting
        time.sleep(0.3)
    
    print(f"Total raw records: {len(all_data)}")
    
    # Remove duplicates by timestamp
    seen_timestamps = set()
    unique_data = []
    for entry in sorted(all_data, key=lambda x: x['t']):
        if entry['t'] not in seen_timestamps:
            seen_timestamps.add(entry['t'])
            unique_data.append(entry)
    
    print(f"Unique records: {len(unique_data)}")
    
    return unique_data

def save_to_csv(data, slug, token_id):
    """Save data to CSV file"""
    if not data:
        print("No data to save!")
        return
    
    csv_file = os.path.join(output_dir, f"{slug}.csv")
    
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Determine all available fields
        available_fields = list(data[0].keys())
        
        # Create header row
        header = ['Timestamp']
        field_mapping = {
            't': 'Timestamp',
            'p': 'Price',
            'v': 'Volume',
            'b': 'Bid',
            'a': 'Ask',
            's': 'Spread'
        }
        
        for field in available_fields:
            if field != 't':
                header.append(field_mapping.get(field, field))
        
        writer.writerow(header)
        
        # Write data rows
        for entry in data:
            row = [datetime.fromtimestamp(entry['t'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')]
            
            for field in available_fields:
                if field != 't':
                    row.append(entry.get(field, ''))
            
            writer.writerow(row)
    
    print(f"Data saved to {csv_file}")
    
    # Print summary
    first_date = datetime.fromtimestamp(data[0]['t'], tz=timezone.utc)
    last_date = datetime.fromtimestamp(data[-1]['t'], tz=timezone.utc)
    print(f"Date range: {first_date} to {last_date}")
    print(f"Total days: {(last_date - first_date).days}")

# Main execution
print(f"Reading markets from {input_csv}...")

markets_to_fetch = []
with open(input_csv, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        if row['tokenid1'] != 'NOT_FOUND' and row['tokenid1']:
            markets_to_fetch.append({
                'slug': row['slug'],
                'tokenid1': row['tokenid1'],
                'tokenid2': row['tokenid2'],
                'conditionId': row['conditionId']
            })

print(f"Found {len(markets_to_fetch)} markets to fetch\n")

# Process each market
for i, market in enumerate(markets_to_fetch, 1):
    print(f"\n{'#'*80}")
    print(f"MARKET {i}/{len(markets_to_fetch)}")
    print(f"{'#'*80}")
    
    try:
        # Fetch data for tokenid1 (Yes outcome)
        data = fetch_market_data(market['tokenid1'], market['slug'])
        
        if data:
            save_to_csv(data, market['slug'], market['tokenid1'])
        else:
            print(f"Skipping {market['slug']} - no data available")
    
    except Exception as e:
        print(f"Error processing {market['slug']}: {e}")
        continue
    
    # Small delay between markets
    time.sleep(1)

print(f"\n{'='*80}")
print("ALL DONE!")
print(f"{'='*80}")
print(f"Processed {len(markets_to_fetch)} markets")
print(f"Data saved in '{output_dir}/' directory")