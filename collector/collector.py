#!/usr/bin/env python3
"""
Polymarket Data Collector
Fetches market data from Polymarket Gamma API and stores in Supabase
"""
from dotenv import load_dotenv
load_dotenv()
import os
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import time
import requests
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
GAMMA_API_BASE = 'https://gamma-api.polymarket.com'

# Initialize Supabase client
def get_supabase_client() -> Client:
    """Initialize and return Supabase client"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


class PolymarketCollector:
    """Collects and stores Polymarket data"""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'PolymarketCollector/1.0'
        })
    
    def get_tracked_markets(self) -> List[Dict[str, str]]:
        """Get list of markets to track from Supabase"""
        try:
            response = self.supabase.table('polymarket_tracked_markets')\
                .select('condition_id, market_slug')\
                .eq('active', True)\
                .execute()
            
            markets = [{'condition_id': row['condition_id'], 'slug': row['market_slug']} 
                      for row in response.data]
            logger.info(f"Found {len(markets)} tracked markets")
            return markets
        except Exception as e:
            logger.error(f"Error fetching tracked markets: {e}")
            return []
    
    def fetch_market_data(self, market_slug: str) -> Optional[Dict[str, Any]]:
        """Fetch market data for a specific market slug"""
        try:
            # Use the direct slug endpoint as shown in the reference code
            url = f"{GAMMA_API_BASE}/markets/slug/{market_slug}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data:
                return data
            else:
                logger.warning(f"No data found for market_slug: {market_slug}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Error fetching market {market_slug}: {e}")
            return None
    
    def fetch_market_by_condition_id(self, condition_id: str) -> Optional[Dict[str, Any]]:
        """Fetch market data by searching through paginated results"""
        try:
            offset = 0
            limit = 100
            max_pages = 10  # Prevent infinite loops
            
            for page in range(max_pages):
                url = f"{GAMMA_API_BASE}/markets"
                params = {'offset': offset, 'limit': limit}
                
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                
                markets = response.json()
                if not markets:
                    break
                
                # Search for our condition_id in the markets
                for market in markets:
                    if market.get('conditionId') == condition_id:
                        logger.info(f"Found market via condition_id: {market.get('slug')}")
                        return market
                
                offset += limit
                time.sleep(0.1)  # Rate limiting
            
            logger.warning(f"Could not find market with condition_id: {condition_id}")
            return None
            
        except requests.RequestException as e:
            logger.error(f"Error searching for market {condition_id}: {e}")
            return None
    
    def fetch_markets_by_slugs(self, slugs: List[str]) -> List[Dict[str, Any]]:
        """Fetch multiple markets by their slugs"""
        markets = []
        for slug in slugs:
            try:
                url = f"{GAMMA_API_BASE}/markets"
                params = {'slug': slug}
                
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                if data and len(data) > 0:
                    markets.append(data[0])
                    
            except requests.RequestException as e:
                logger.error(f"Error fetching market {slug}: {e}")
                
        return markets
    
    def _parse_json_field(self, field: Any) -> Optional[Any]:
        """Parse JSON field if it's a string"""
        if isinstance(field, str):
            try:
                return json.loads(field)
            except json.JSONDecodeError:
                return None
        return field
    
    def _to_float(self, value: Any) -> Optional[float]:
        """Convert value to float, handling None and invalid values"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _to_int(self, value: Any) -> Optional[int]:
        """Convert value to int, handling None and invalid values"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_timestamp(self, ts_str: Any) -> Optional[str]:
        """Parse timestamp string to ISO format"""
        if not ts_str:
            return None
        if isinstance(ts_str, str):
            return ts_str
        return None
    
    def parse_market_data(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """Parse market data into database format with all available fields"""
        
        # Parse JSON fields
        outcome_prices = self._parse_json_field(market.get('outcomePrices'))
        outcomes = self._parse_json_field(market.get('outcomes'))
        clob_token_ids = self._parse_json_field(market.get('clobTokenIds'))
        
        return {
            # Core identifiers
            'condition_id': market.get('conditionId'),
            'market_slug': market.get('slug'),
            'question': market.get('question'),
            'snapshot_timestamp': datetime.now(timezone.utc).isoformat(),
            
            # Market status
            'active': market.get('active'),
            'closed': market.get('closed'),
            'archived': market.get('archived'),
            'restricted': market.get('restricted'),
            'neg_risk': market.get('negRisk', False),
            'accepting_orders': market.get('acceptingOrders'),
            
            # Core metrics
            'volume': self._to_float(market.get('volume')),
            'liquidity': self._to_float(market.get('liquidity')),
            'open_interest': self._to_float(market.get('openInterest')),
            
            # Volume breakdowns
            'volume_24hr': self._to_float(market.get('volume24hr')),
            'volume_1wk': self._to_float(market.get('volume1wk')),
            'volume_1mo': self._to_float(market.get('volume1mo')),
            'volume_1yr': self._to_float(market.get('volume1yr')),
            
            # CLOB-specific volumes
            'volume_clob': self._to_float(market.get('volumeClob')),
            'volume_24hr_clob': self._to_float(market.get('volume24hrClob')),
            'volume_1wk_clob': self._to_float(market.get('volume1wkClob')),
            'volume_1mo_clob': self._to_float(market.get('volume1moClob')),
            'volume_1yr_clob': self._to_float(market.get('volume1yrClob')),
            
            # Liquidity metrics
            'liquidity_num': self._to_float(market.get('liquidityNum')),
            'liquidity_clob': self._to_float(market.get('liquidityClob')),
            
            # Price data
            'outcome_prices': outcome_prices,
            'last_trade_price': self._to_float(market.get('lastTradePrice')),
            'best_bid': self._to_float(market.get('bestBid')),
            'best_ask': self._to_float(market.get('bestAsk')),
            'spread': self._to_float(market.get('spread')),
            
            # Price changes
            'one_hour_price_change': self._to_float(market.get('oneHourPriceChange')),
            'one_day_price_change': self._to_float(market.get('oneDayPriceChange')),
            'one_week_price_change': self._to_float(market.get('oneWeekPriceChange')),
            'one_month_price_change': self._to_float(market.get('oneMonthPriceChange')),
            
            # Market metadata
            'outcomes': outcomes,
            'clob_token_ids': clob_token_ids,
            'market_type': market.get('marketType'),
            'category': market.get('category'),
            'description': market.get('description'),
            'image_url': market.get('image'),
            'icon_url': market.get('icon'),
            
            # Dates
            'start_date': self._parse_timestamp(market.get('startDate')),
            'end_date': self._parse_timestamp(market.get('endDate')),
            'accepting_orders_timestamp': self._parse_timestamp(market.get('acceptingOrdersTimestamp')),
            
            # Trading parameters
            'order_price_min_tick_size': self._to_float(market.get('orderPriceMinTickSize')),
            'order_min_size': self._to_float(market.get('orderMinSize')),
            'rewards_min_size': self._to_float(market.get('rewardsMinSize')),
            'rewards_max_spread': self._to_float(market.get('rewardsMaxSpread')),
            
            # Market quality metrics
            'competitive': self._to_float(market.get('competitive')),
            'comment_count': self._to_int(market.get('commentCount')),
            
            # UMA resolution
            'uma_bond': self._to_float(market.get('umaBond')),
            'uma_reward': self._to_float(market.get('umaReward')),
            
            # Flags
            'enable_order_book': market.get('enableOrderBook'),
            'cyom': market.get('cyom'),
            'featured': market.get('featured'),
            'new': market.get('new'),
            'approved': market.get('approved'),
            
            # Timestamps
            'updated_at': self._parse_timestamp(market.get('updatedAt'))
        }
    
    def store_snapshot(self, parsed_data: Dict[str, Any]) -> bool:
        """Store market snapshot in Supabase"""
        try:
            response = self.supabase.table('polymarket_snapshots').insert(parsed_data).execute()
            logger.info(f"Stored snapshot for market: {parsed_data['market_slug']}")
            return True
        except Exception as e:
            logger.error(f"Error storing snapshot: {e}")
            return False
    
    def collect_all(self) -> Dict[str, int]:
        """Main collection function"""
        logger.info("Starting Polymarket data collection")
        
        # Get tracked markets
        markets = self.get_tracked_markets()
        
        if not markets:
            logger.warning("No tracked markets found. Add markets to polymarket_tracked_markets table.")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        success_count = 0
        failed_count = 0
        
        for market in markets:
            market_slug = market['slug']
            condition_id = market['condition_id']
            logger.info(f"Processing market: {market_slug} (condition_id: {condition_id})")
            
            # Fetch market data - try slug first, fallback to condition_id search
            market_data = self.fetch_market_data(market_slug)
            
            if not market_data:
                logger.info(f"Slug fetch failed, trying condition_id search...")
                market_data = self.fetch_market_by_condition_id(condition_id)
            
            if not market_data:
                failed_count += 1
                continue
            
            # Parse and store
            parsed_data = self.parse_market_data(market_data)
            
            if self.store_snapshot(parsed_data):
                success_count += 1
            else:
                failed_count += 1
        
        stats = {
            'success': success_count,
            'failed': failed_count,
            'total': len(markets)
        }
        
        logger.info(f"Collection complete: {stats}")
        return stats


def main():
    """Main entry point"""
    try:
        collector = PolymarketCollector()
        stats = collector.collect_all()
        
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