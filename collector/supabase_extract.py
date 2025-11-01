#!/usr/bin/env python3
"""
Export ALL rows for a given condition_id from Supabase/PostgREST
into polymarket_prices_full.csv with format:
Timestamp,Price
YYYY-MM-DD HH:MM:SS,price
"""

import os
import csv
import sys
from datetime import datetime
import time
import requests
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

CONDITION_ID = "0x05af636e0989accb08334a74f69e5368e0aa28fe498fd16a3ca991e6dc5ae2cc"
OUTPUT_FILE = "polymarket_prices.csv"
PAGE_SIZE = 1000  # Default (and max) page size for Supabase/PostgREST

# Build the REST endpoint URL
endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/polymarket_price_history"

# Headers for Supabase REST API
headers = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Accept": "application/json",
    "Range-Unit": "items",
    # The 'Range' header will be set dynamically inside the loop
}

params = {
    "select": "timestamp,price",
    "condition_id": f"eq.{CONDITION_ID}",
    "order": "timestamp.asc"
}

all_data = []
offset = 0

print("Fetching data from Supabase in pages...")

while True:
    # Calculate the range for the current page
    start_row = offset
    end_row = offset + PAGE_SIZE - 1
    headers["Range"] = f"{start_row}-{end_row}"

    print(f"Fetching rows {start_row} to {end_row}...")
    
    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=60)

        # Check for non-successful status codes
        if response.status_code not in (200, 206):
            # 416 means "Range Not Satisfiable," i.e., we've asked for rows that don't exist
            if response.status_code == 416:
                print("Reached the end of the data.")
                break
            # Other errors
            raise RuntimeError(f"Failed: {response.status_code} {response.text}")

        data = response.json()

        # If no data is returned, we're done
        if not data:
            print("No more data found.")
            break
        
        # Add the data from this page to our master list
        all_data.extend(data)
        print(f"Fetched {len(data)} new rows. Total so far: {len(all_data)}")

        # If the number of rows returned is less than we asked for,
        # it means this is the last page.
        if len(data) < PAGE_SIZE:
            print("Reached the last page.")
            break
        
        # Prepare for the next page
        offset += PAGE_SIZE
        
        # Optional: add a small delay to be polite to the API
        # time.sleep(0.1)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


print(f"\nTotal rows fetched: {len(all_data)}")

# Write all collected data to the CSV
with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Timestamp", "Price"])
    # Iterate over all_data, not just 'data'
    for row in all_data:
        ts_raw = row.get("timestamp")
        price = row.get("price")
        if ts_raw:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            formatted = ts.strftime("%Y-%m-%d %H:%M:%S")
        else:
            formatted = ""
        writer.writerow([formatted, price])

print(f"✅ Exported {len(all_data)} rows to {OUTPUT_FILE}")