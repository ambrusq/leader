"""
Kalshi Market Tracker Setup
Adds markets to Supabase tracking from Kalshi URLs
Similar to the historical data fetcher but stores market metadata in database
"""

from dotenv import load_dotenv
load_dotenv()
import os
import requests
import re
from datetime import datetime
from typing import List, Dict, Optional
import logging
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Market URLs to track - ADD YOUR URLS HERE
MARKET_URLS = [
    "https://kalshi.com/markets/kxchinausgdp/china-overtakes-us-gdp/chinausgdp",
    "https://kalshi.com/markets/kxgdpyear/annual-gdp/kxgdpyear-25",
    "https://kalshi.com/markets/kxgdp/us-gdp-growth/kxgdp-25oct30",
    "https://kalshi.com/markets/kxtrumpputin2/trump-putin-meet-again-this-year/kxtrumpputin2-26jan01",
    "https://kalshi.com/markets/kxdjtvostariffs/tariffs-case/kxdjtvostariffs",
    "https://kalshi.com/markets/kxbtcvsgold/btc-vs-gold/kxbtcvsgold-25",
    "https://kalshi.com/markets/kxtariffrateprc/tariff-rate-china/kxtariffrateprc-26jan01",
    "https://kalshi.com/markets/kxwrecss/world-recessions/wrecss-26",
    "https://kalshi.com/markets/kxratecutcount/number-of-rate-cuts/kxratecutcount-25dec31",
]


class KalshiMarketAdder:
    """Adds Kalshi markets to Supabase tracking"""
    
    def __init__(self):
        """Initialize with Supabase connection"""
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.base_url = BASE_URL
    
    def extract_ticker_from_url(self, url: str) -> Optional[str]:
        """Extract the ticker from a Kalshi URL"""
        match = re.search(r'/([^/]+)$', url)
        return match.group(1).upper() if match else None
    
    def extract_event_ticker_from_url(self, url: str) -> Optional[str]:
        """Extract the event ticker from a Kalshi URL"""
        parts = url.rstrip('/').split('/')
        if len(parts) >= 2:
            return parts[-1].upper()
        return None
    
    def fetch_market_data(self, ticker: str) -> Optional[Dict]:
        """Fetch market data from Kalshi API"""
        url = f"{self.base_url}/markets/{ticker}"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json().get('market')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching market {ticker}: {e}")
            return None
    
    def fetch_event_data(self, event_ticker: str) -> List[Dict]:
        """Fetch event data and return all markets"""
        url = f"{self.base_url}/events/{event_ticker}"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get('event'), data.get('markets', [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching event {event_ticker}: {e}")
            return None, []
    
    def extract_series_ticker(self, event_ticker: str) -> str:
        """Extract series ticker from event ticker"""
        match = re.match(r'^([A-Z]+)', event_ticker)
        return match.group(1) if match else event_ticker
    
    def store_event(self, event_data: Dict) -> Optional[int]:
        """Store event in database and return event_id"""
        if not event_data:
            return None
        
        event_ticker = event_data.get('event_ticker')
        if not event_ticker:
            return None
        
        try:
            # Check if event already exists
            result = self.supabase.table('kalshi_events').select('id').eq(
                'event_ticker', event_ticker
            ).execute()
            
            if result.data:
                logger.info(f"Event {event_ticker} already exists")
                return result.data[0]['id']
            
            # Insert new event
            event_record = {
                'event_ticker': event_ticker,
                'series_ticker': self.extract_series_ticker(event_ticker),
                'title': event_data.get('title'),
                'sub_title': event_data.get('sub_title'),
                'category': event_data.get('category'),
                'strike_date': event_data.get('strike_date'),
                'mutually_exclusive': event_data.get('mutually_exclusive', False),
                'active': True
            }
            
            result = self.supabase.table('kalshi_events').insert(event_record).execute()
            
            if result.data:
                event_id = result.data[0]['id']
                logger.info(f"Created event {event_ticker} with ID {event_id}")
                return event_id
            
        except Exception as e:
            logger.error(f"Error storing event {event_ticker}: {e}")
        
        return None
    
    def store_market(self, market_data: Dict, event_id: Optional[int] = None) -> bool:
        """Store market in database"""
        ticker = market_data.get('ticker')
        if not ticker:
            return False
        
        try:
            # Check if market already exists
            result = self.supabase.table('kalshi_tracked_markets').select('ticker').eq(
                'ticker', ticker
            ).execute()
            
            if result.data:
                logger.info(f"Market {ticker} already tracked, updating...")
            
            event_ticker = market_data.get('event_ticker', '')
            
            market_record = {
                'ticker': ticker,
                'event_ticker': event_ticker,
                'series_ticker': self.extract_series_ticker(event_ticker),
                'title': market_data.get('title'),
                'subtitle': market_data.get('subtitle'),
                'yes_sub_title': market_data.get('yes_sub_title'),
                'no_sub_title': market_data.get('no_sub_title'),
                'floor_strike': market_data.get('floor_strike'),
                'strike_type': market_data.get('strike_type'),
                'open_time': market_data.get('open_time'),
                'close_time': market_data.get('close_time'),
                'expiration_time': market_data.get('expiration_time'),
                'status': market_data.get('status'),
                'market_type': market_data.get('market_type'),
                'category': market_data.get('category'),
                'rules_primary': market_data.get('rules_primary'),
                'event_id': event_id,
                'active': True
            }
            
            # Upsert (insert or update)
            result = self.supabase.table('kalshi_tracked_markets').upsert(
                market_record,
                on_conflict='ticker'
            ).execute()
            
            logger.info(f"✓ Stored market {ticker}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing market {ticker}: {e}")
            return False
    
    def process_url(self, url: str) -> Dict:
        """Process a single URL and add markets to tracking"""
        logger.info(f"\nProcessing URL: {url}")
        ticker = self.extract_ticker_from_url(url)
        
        if not ticker:
            logger.error(f"  Could not extract ticker from URL")
            return {'url': url, 'status': 'error', 'message': 'Invalid URL'}
        
        markets_added = 0
        
        # Try as market first
        market_data = self.fetch_market_data(ticker)
        
        if market_data:
            logger.info(f"  ✓ Found as market: {ticker}")
            
            # Try to get and store event data
            event_ticker = market_data.get('event_ticker')
            event_id = None
            
            if event_ticker:
                event_data, _ = self.fetch_event_data(event_ticker)
                event_id = self.store_event(event_data)
            
            # Store the market
            if self.store_market(market_data, event_id):
                markets_added = 1
            
            return {
                'url': url,
                'status': 'success',
                'type': 'market',
                'ticker': ticker,
                'markets_added': markets_added
            }
        else:
            # Try as event
            logger.info(f"  Not a market, trying as event...")
            event_ticker = self.extract_event_ticker_from_url(url)
            event_data, markets = self.fetch_event_data(event_ticker)
            
            if not markets:
                logger.error(f"  ✗ Could not fetch data for {ticker}")
                return {
                    'url': url,
                    'status': 'error',
                    'message': 'No data found'
                }
            
            logger.info(f"  ✓ Found as event with {len(markets)} markets")
            
            # Store event
            event_id = self.store_event(event_data)
            
            # Store all markets
            for market in markets:
                if self.store_market(market, event_id):
                    markets_added += 1
            
            return {
                'url': url,
                'status': 'success',
                'type': 'event',
                'event_ticker': event_ticker,
                'markets_added': markets_added,
                'total_markets': len(markets)
            }
    
    def add_all_markets(self, urls: List[str]) -> Dict:
        """Process all URLs and add markets to tracking"""
        logger.info(f"Starting to add {len(urls)} URL(s) to tracking")
        logger.info(f"{'='*60}\n")
        
        results = []
        total_markets_added = 0
        
        for url in urls:
            try:
                result = self.process_url(url)
                results.append(result)
                total_markets_added += result.get('markets_added', 0)
            except Exception as e:
                logger.error(f"Error processing URL {url}: {e}", exc_info=True)
                results.append({
                    'url': url,
                    'status': 'error',
                    'message': str(e)
                })
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Completed: {total_markets_added} markets added to tracking")
        logger.info(f"{'='*60}")
        
        return {
            'total_urls_processed': len(urls),
            'total_markets_added': total_markets_added,
            'results': results
        }
    
    def list_tracked_markets(self) -> List[Dict]:
        """List all currently tracked markets"""
        try:
            result = self.supabase.table('kalshi_tracked_markets').select(
                'ticker, title, status, category, open_time, close_time, active'
            ).eq('active', True).order('ticker').execute()
            
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Error listing tracked markets: {e}")
            return []
    
    def deactivate_market(self, ticker: str) -> bool:
        """Deactivate a market (stop tracking)"""
        try:
            result = self.supabase.table('kalshi_tracked_markets').update({
                'active': False
            }).eq('ticker', ticker).execute()
            
            logger.info(f"Deactivated market {ticker}")
            return True
        except Exception as e:
            logger.error(f"Error deactivating market {ticker}: {e}")
            return False


def main():
    """Main function to add markets to tracking"""
    adder = KalshiMarketAdder()
    
    # Add all markets from URLs
    results = adder.add_all_markets(MARKET_URLS)
    
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total URLs processed: {results['total_urls_processed']}")
    print(f"Total markets added: {results['total_markets_added']}")
    print(f"\nCurrently tracked markets:")
    
    # List all tracked markets
    tracked = adder.list_tracked_markets()
    if tracked:
        print(f"\nFound {len(tracked)} active markets:")
        for market in tracked:
            print(f"  - {market['ticker']}: {market['title']}")
            print(f"    Status: {market['status']}, Category: {market['category']}")
    else:
        print("  No active markets found")


if __name__ == "__main__":
    main()