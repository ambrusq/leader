#!/usr/bin/env python3
"""
Polymarket Price Data Collector
Fetches minute-by-minute price history and stores in Supabase
"""
from dotenv import load_dotenv
load_dotenv()
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import time
import requests
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
CLOB_API_BASE = 'https://clob.polymarket.com'

def get_supabase_client() -> Client:
    """Initialize and return Supabase client"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


class PolymarketPriceCollector:
    """Collects minute-by-minute price data for tracked markets"""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'PolymarketPriceCollector/1.0'
        })
        self.fidelity = 1  # 1 = minute data
    
    def get_tracked_markets_with_tokens(self) -> List[Dict[str, Any]]:
        """Get tracked markets with their token IDs"""
        try:
            # First get tracked markets
            tracked_response = self.supabase.table('polymarket_tracked_markets')\
                .select('condition_id, market_slug')\
                .eq('active', True)\
                .execute()
            
            markets_with_tokens = []
            
            # For each tracked market, get token IDs from latest snapshot
            for market in tracked_response.data:
                snapshot_response = self.supabase.table('polymarket_snapshots')\
                    .select('clob_token_ids')\
                    .eq('condition_id', market['condition_id'])\
                    .order('snapshot_timestamp', desc=True)\
                    .limit(1)\
                    .execute()
                
                if snapshot_response.data and snapshot_response.data[0]['clob_token_ids']:
                    token_ids = snapshot_response.data[0]['clob_token_ids']
                    
                    # Handle both list and JSON string formats
                    if isinstance(token_ids, str):
                        token_ids = json.loads(token_ids)
                    
                    if token_ids and len(token_ids) > 0:
                        markets_with_tokens.append({
                            'condition_id': market['condition_id'],
                            'market_slug': market['market_slug'],
                            'token_id': token_ids[0]  # Use first token (Yes outcome)
                        })
            
            logger.info(f"Found {len(markets_with_tokens)} markets with token IDs")
            return markets_with_tokens
            
        except Exception as e:
            logger.error(f"Error fetching tracked markets: {e}")
            return []
    
    def get_last_price_timestamp(self, token_id: str) -> Optional[datetime]:
        """Get the timestamp of the last stored price for a token"""
        try:
            response = self.supabase.table('polymarket_price_history')\
                .select('timestamp')\
                .eq('token_id', token_id)\
                .order('timestamp', desc=True)\
                .limit(1)\
                .execute()
            
            if response.data:
                timestamp_str = response.data[0]['timestamp']
                # Parse ISO format timestamp
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting last timestamp for {token_id}: {e}")
            return None
    
    def fetch_price_history(
        self, 
        token_id: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch price history from CLOB API"""
        try:
            url = f"{CLOB_API_BASE}/prices-history"
            
            # Convert to timestamps
            start_ts = int(start_date.timestamp())
            end_ts = int(end_date.timestamp())
            
            params = {
                'market': token_id,
                'startTs': start_ts,
                'endTs': end_ts,
                'fidelity': self.fidelity
            }
            
            logger.info(f"Fetching prices for {token_id} from {start_date} to {end_date}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'history' in data and len(data['history']) > 0:
                logger.info(f"Retrieved {len(data['history'])} price points")
                return data['history']
            else:
                logger.warning(f"No price data found for {token_id}")
                return []
                
        except requests.RequestException as e:
            logger.error(f"Error fetching prices for {token_id}: {e}")
            return []
    
    def fetch_price_history_chunked(
        self,
        token_id: str,
        start_date: datetime,
        end_date: datetime,
        chunk_days: int = 7
    ) -> List[Dict[str, Any]]:
        """Fetch price history in chunks to avoid API limits"""
        all_data = []
        current_start = start_date
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)
            
            chunk_data = self.fetch_price_history(token_id, current_start, current_end)
            
            if chunk_data:
                all_data.extend(chunk_data)
            
            current_start = current_end
            time.sleep(0.3)  # Rate limiting
        
        # Remove duplicates by timestamp
        seen_timestamps = set()
        unique_data = []
        for entry in sorted(all_data, key=lambda x: x['t']):
            if entry['t'] not in seen_timestamps:
                seen_timestamps.add(entry['t'])
                unique_data.append(entry)
        
        return unique_data
    
    def store_price_data(
        self, 
        condition_id: str, 
        token_id: str, 
        price_data: List[Dict[str, Any]]
    ) -> int:
        """Store price data in Supabase, returns count of stored records"""
        if not price_data:
            return 0
        
        try:
            # Prepare records for insertion
            records = []
            for entry in price_data:
                records.append({
                    'condition_id': condition_id,
                    'token_id': token_id,
                    'timestamp': datetime.fromtimestamp(entry['t'], tz=timezone.utc).isoformat(),
                    'price': float(entry['p']) if entry.get('p') is not None else None
                })
            
            # Batch insert (Supabase handles upsert with unique constraint)
            # Insert in chunks of 1000 to avoid payload limits
            chunk_size = 1000
            total_inserted = 0
            
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                self.supabase.table('polymarket_price_history').insert(chunk).execute()
                total_inserted += len(chunk)
                logger.info(f"Inserted {len(chunk)} price records (total: {total_inserted}/{len(records)})")
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"Error storing price data: {e}")
            return 0
    
    def collect_prices_for_market(
        self, 
        market: Dict[str, Any],
        lookback_hours: int = 12  # Default lookback if no existing data
    ) -> Dict[str, Any]:
        """Collect prices for a single market"""
        condition_id = market['condition_id']
        token_id = market['token_id']
        market_slug = market['market_slug']
        
        logger.info(f"Processing market: {market_slug}")
        
        # Get last stored timestamp
        last_timestamp = self.get_last_price_timestamp(token_id)
        
        # Determine start date
        if last_timestamp:
            # Start from last timestamp + 1 minute to avoid duplicates
            start_date = last_timestamp + timedelta(minutes=1)
            logger.info(f"Last data at {last_timestamp}, fetching from {start_date}")
        else:
            # No existing data, fetch last N hours
            start_date = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
            logger.info(f"No existing data, fetching last {lookback_hours} hours")
        
        end_date = datetime.now(timezone.utc)
        
        # Check if we need to fetch anything
        if start_date >= end_date:
            logger.info(f"Data is up to date for {market_slug}")
            return {
                'market_slug': market_slug,
                'status': 'up_to_date',
                'records_added': 0
            }
        
        # Fetch price data
        price_data = self.fetch_price_history_chunked(token_id, start_date, end_date)
        
        if not price_data:
            logger.warning(f"No new price data for {market_slug}")
            return {
                'market_slug': market_slug,
                'status': 'no_data',
                'records_added': 0
            }
        
        # Store price data
        records_added = self.store_price_data(condition_id, token_id, price_data)
        
        return {
            'market_slug': market_slug,
            'status': 'success',
            'records_added': records_added,
            'date_range': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat()
            }
        }
    
    def collect_all_prices(self, lookback_hours: int = 12) -> Dict[str, Any]:
        """Main collection function for all tracked markets"""
        logger.info("Starting price data collection")
        
        # Get tracked markets with token IDs
        markets = self.get_tracked_markets_with_tokens()
        
        if not markets:
            logger.warning("No tracked markets with token IDs found")
            return {
                'status': 'error',
                'message': 'No markets to process',
                'markets_processed': 0,
                'total_records_added': 0
            }
        
        results = []
        total_records = 0
        
        for market in markets:
            try:
                result = self.collect_prices_for_market(market, lookback_hours)
                results.append(result)
                total_records += result.get('records_added', 0)
                
                # Small delay between markets
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing {market['market_slug']}: {e}")
                results.append({
                    'market_slug': market['market_slug'],
                    'status': 'error',
                    'error': str(e)
                })
        
        stats = {
            'status': 'success',
            'markets_processed': len(results),
            'total_records_added': total_records,
            'results': results
        }
        
        logger.info(f"Price collection complete: {stats}")
        return stats


def main():
    """Main entry point"""
    try:
        collector = PolymarketPriceCollector()
        stats = collector.collect_all_prices()
        
        print(json.dumps({
            'status': 'success',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'stats': stats
        }))
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(json.dumps({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }))
        exit(1)


if __name__ == '__main__':
    main()