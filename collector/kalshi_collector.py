"""
Kalshi Price Collector
Fetches latest price data for tracked Kalshi markets and stores in Supabase
"""
from dotenv import load_dotenv
load_dotenv()
import os
import requests
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional
import logging
from supabase import create_client, Client
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KalshiCollector:
    """Collects price data from Kalshi API and stores in Supabase"""
    
    def __init__(self):
        """Initialize the collector with Supabase connection"""
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.candlestick_limit = 4900  # Stay under 5000 limit
    
    def get_active_markets(self) -> List[Dict]:
        """Get all active tracked markets from Supabase"""
        try:
            # Corrected line
            response = self.supabase.rpc('get_active_kalshi_markets', {}).execute()
            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error fetching active markets: {e}")
            return []
    
    def fetch_candlesticks(self, series_ticker: str, market_ticker: str, 
                          start_ts: int, end_ts: int) -> List[Dict]:
        """Fetch candlestick data from Kalshi API"""
        url = f"{self.base_url}/series/{series_ticker}/markets/{market_ticker}/candlesticks"
        params = {
            'start_ts': start_ts,
            'end_ts': end_ts,
            'period_interval': 1  # 1 minute intervals
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json().get('candlesticks', [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching candlesticks for {market_ticker}: {e}")
            return []
    
    def get_all_candlesticks(self, series_ticker: str, market_ticker: str,
                            start_ts: int, end_ts: int) -> List[Dict]:
        """Fetch all candlesticks, handling the 5000 limit with multiple requests"""
        all_candlesticks = []
        current_start = start_ts
        
        # Each candlestick is 1 minute (60 seconds)
        chunk_duration = self.candlestick_limit * 60
        
        while current_start < end_ts:
            current_end = min(current_start + chunk_duration, end_ts)
            
            candlesticks = self.fetch_candlesticks(
                series_ticker, market_ticker, current_start, current_end
            )
            
            if candlesticks:
                all_candlesticks.extend(candlesticks)
                logger.debug(f"  Retrieved {len(candlesticks)} candlesticks for {market_ticker}")
            
            current_start = current_end
            time.sleep(0.2)  # Rate limiting
        
        return all_candlesticks
    
    def transform_candlestick(self, ticker: str, candle: Dict) -> Dict:
        """Transform candlestick data for database insertion"""
        timestamp = candle.get('end_period_ts')
        
        return {
            'ticker': ticker,
            'timestamp': datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else None,
            'end_period_ts': timestamp,
            'open_interest': candle.get('open_interest'),
            'volume': candle.get('volume'),
            # Price data (trades)
            'price_open': candle.get('price', {}).get('open'),
            'price_close': candle.get('price', {}).get('close'),
            'price_high': candle.get('price', {}).get('high'),
            'price_low': candle.get('price', {}).get('low'),
            'price_mean': candle.get('price', {}).get('mean'),
            # Yes ask data
            'yes_ask_open': candle.get('yes_ask', {}).get('open'),
            'yes_ask_close': candle.get('yes_ask', {}).get('close'),
            'yes_ask_high': candle.get('yes_ask', {}).get('high'),
            'yes_ask_low': candle.get('yes_ask', {}).get('low'),
            # Yes bid data
            'yes_bid_open': candle.get('yes_bid', {}).get('open'),
            'yes_bid_close': candle.get('yes_bid', {}).get('close'),
            'yes_bid_high': candle.get('yes_bid', {}).get('high'),
            'yes_bid_low': candle.get('yes_bid', {}).get('low'),
        }
    
    def store_price_data(self, price_data: List[Dict]) -> int:
        """Store price data in Supabase (batch upsert)"""
        if not price_data:
            return 0
        
        try:
            # Supabase upsert - will update if exists, insert if not
            response = self.supabase.table('kalshi_price_history').upsert(
                price_data,
                on_conflict='ticker,end_period_ts'
            ).execute()
            
            return len(price_data)
        except Exception as e:
            logger.error(f"Error storing price data: {e}")
            return 0
    
    def collect_market_prices(self, market: Dict) -> Dict:
        """Collect prices for a single market"""
        ticker = market['ticker']
        series_ticker = market['series_ticker']
        open_time = market['open_time']
        last_price_timestamp = market.get('last_price_timestamp')
        
        # Determine start timestamp
        if last_price_timestamp:
            # Start from last collected timestamp
            start_dt = datetime.fromisoformat(last_price_timestamp.replace('Z', '+00:00'))
            start_ts = int(start_dt.timestamp())
        else:
            # Start from 12 hours ago
            start_ts = int((datetime.now(timezone.utc) - timedelta(hours=12)).timestamp())
        
        # End at current time
        end_ts = int(datetime.now(timezone.utc).timestamp())
        
        # Skip if already up to date (within 2 minutes)
        if end_ts - start_ts < 120:
            logger.info(f"Market {ticker} is already up to date")
            return {'ticker': ticker, 'records': 0, 'status': 'up_to_date'}
        
        logger.info(f"Collecting data for {ticker} from {datetime.fromtimestamp(start_ts)} to now")
        
        # Fetch candlesticks
        candlesticks = self.get_all_candlesticks(series_ticker, ticker, start_ts, end_ts)
        
        if not candlesticks:
            logger.warning(f"No candlesticks found for {ticker}")
            return {'ticker': ticker, 'records': 0, 'status': 'no_data'}
        
        # Transform data
        price_data = [self.transform_candlestick(ticker, c) for c in candlesticks]
        
        # Store in database
        stored_count = self.store_price_data(price_data)
        
        logger.info(f"Stored {stored_count} records for {ticker}")
        
        return {
            'ticker': ticker,
            'records': stored_count,
            'status': 'success',
            'start_time': datetime.fromtimestamp(start_ts).isoformat(),
            'end_time': datetime.fromtimestamp(end_ts).isoformat()
        }
    
    def collect_all_prices(self) -> Dict:
        """Collect prices for all active markets"""
        logger.info("Starting Kalshi price collection")
        start_time = datetime.now(timezone.utc)
        
        # Get active markets
        markets = self.get_active_markets()
        logger.info(f"Found {len(markets)} active markets to update")
        
        if not markets:
            return {
                'status': 'success',
                'markets_processed': 0,
                'total_records': 0,
                'duration_seconds': 0
            }
        
        # Collect prices for each market
        results = []
        total_records = 0
        
        for market in markets:
            try:
                result = self.collect_market_prices(market)
                results.append(result)
                total_records += result.get('records', 0)
                
                # Rate limiting between markets
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing market {market.get('ticker')}: {e}", exc_info=True)
                results.append({
                    'ticker': market.get('ticker'),
                    'records': 0,
                    'error': str(e)
                })
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Collection completed: {len(markets)} markets, {total_records} records, {duration:.2f}s")
        
        return {
            'status': 'success',
            'markets_processed': len(markets),
            'total_records': total_records,
            'duration_seconds': duration,
            'results': results,
            'timestamp': end_time.isoformat()
        }
    
    def add_market_to_tracking(self, ticker: str, fetch_metadata: bool = True) -> Dict:
        """Add a new market to tracking"""
        try:
            # Fetch market metadata from Kalshi
            if fetch_metadata:
                url = f"{self.base_url}/markets/{ticker}"
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                market_data = response.json().get('market', {})
            else:
                return {'error': 'Market metadata required'}
            
            event_ticker = market_data.get('event_ticker')
            
            # Check if event exists, if not create it
            event_id = None
            if event_ticker:
                event_result = self.supabase.table('kalshi_events').select('id').eq(
                    'event_ticker', event_ticker
                ).execute()
                
                if event_result.data:
                    event_id = event_result.data[0]['id']
                else:
                    # Create event (will need to fetch event data separately)
                    logger.info(f"Event {event_ticker} not found, creating placeholder")
                    # You may want to fetch event data here
            
            # Insert market
            market_record = {
                'ticker': ticker,
                'event_ticker': event_ticker,
                'series_ticker': market_data.get('event_ticker', '').split('-')[0] if event_ticker else None,
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
            
            result = self.supabase.table('kalshi_tracked_markets').upsert(
                market_record,
                on_conflict='ticker'
            ).execute()
            
            logger.info(f"Added market {ticker} to tracking")
            return {'status': 'success', 'ticker': ticker}
            
        except Exception as e:
            logger.error(f"Error adding market {ticker}: {e}")
            return {'error': str(e)}


if __name__ == "__main__":
    # Test the collector
    collector = KalshiCollector()
    stats = collector.collect_all_prices()
    print(f"Collection stats: {stats}")