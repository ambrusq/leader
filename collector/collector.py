#!/usr/bin/env python3
"""
Enhanced Polymarket Data Collector
Handles both individual markets and multi-market events
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
GAMMA_API_BASE = 'https://gamma-api.polymarket.com'

def get_supabase_client() -> Client:
    """Initialize and return Supabase client"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


class PolymarketCollector:
    """Collects and stores Polymarket data with event support"""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'PolymarketCollector/2.0'
        })
    
    def fetch_event_data(self, event_slug: str) -> Optional[Dict[str, Any]]:
        """Fetch event data including all its markets"""
        try:
            url = f"{GAMMA_API_BASE}/events/slug/{event_slug}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching event {event_slug}: {e}")
            return None
    
    def fetch_market_data(self, market_slug: str) -> Optional[Dict[str, Any]]:
        """Fetch market data for a specific market slug"""
        try:
            url = f"{GAMMA_API_BASE}/markets/slug/{market_slug}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data if data else None
        except requests.RequestException as e:
            logger.error(f"Error fetching market {market_slug}: {e}")
            return None
    
    def get_or_create_event(self, event_data: Dict[str, Any]) -> Optional[int]:
        """Store or update event in database and return event_id"""
        try:
            event_slug = event_data.get('slug')
            if not event_slug:
                return None
            
            # Check if event exists
            existing = self.supabase.table('polymarket_events')\
                .select('id')\
                .eq('event_slug', event_slug)\
                .execute()
            
            event_record = {
                'event_slug': event_slug,
                'title': event_data.get('title'),
                'description': event_data.get('description'),
                'category': event_data.get('category'),
                'image_url': event_data.get('image'),
                'icon_url': event_data.get('icon'),
                'start_date': self._parse_timestamp(event_data.get('startDate')),
                'end_date': self._parse_timestamp(event_data.get('endDate')),
                'closed': event_data.get('closed', False),
                'active': event_data.get('active', True),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            if existing.data:
                # Update existing event
                event_id = existing.data[0]['id']
                self.supabase.table('polymarket_events')\
                    .update(event_record)\
                    .eq('id', event_id)\
                    .execute()
                logger.info(f"Updated event: {event_slug}")
            else:
                # Create new event
                result = self.supabase.table('polymarket_events')\
                    .insert(event_record)\
                    .execute()
                event_id = result.data[0]['id']
                logger.info(f"Created event: {event_slug}")
            
            return event_id
            
        except Exception as e:
            logger.error(f"Error managing event: {e}")
            return None
    
    def sync_event_markets(self, event_slug: str) -> List[str]:
        """
        Fetch event data and sync all its markets to tracked_markets table.
        Returns list of market slugs that were synced.
        """
        event_data = self.fetch_event_data(event_slug)
        if not event_data:
            logger.warning(f"Could not fetch event: {event_slug}")
            return []
        
        # Store/update event
        event_id = self.get_or_create_event(event_data)
        if not event_id:
            logger.warning(f"Could not create/update event: {event_slug}")
            return []
        
        # Determine event type based on markets
        markets = event_data.get('markets', [])
        event_type = 'multi_outcome' if len(markets) > 1 else 'single'
        
        # Update event type
        try:
            self.supabase.table('polymarket_events')\
                .update({'event_type': event_type})\
                .eq('id', event_id)\
                .execute()
        except Exception as e:
            logger.error(f"Error updating event type: {e}")
        
        synced_slugs = []
        
        # Sync each market
        for market in markets:
            try:
                condition_id = market.get('conditionId')
                market_slug = market.get('slug')
                
                if not condition_id or not market_slug:
                    continue
                
                # Check if market already tracked
                existing = self.supabase.table('polymarket_tracked_markets')\
                    .select('id')\
                    .eq('condition_id', condition_id)\
                    .execute()
                
                market_record = {
                    'condition_id': condition_id,
                    'market_slug': market_slug,
                    'event_id': event_id,
                    'market_title': market.get('question'),
                    'outcome_label': market.get('outcomes', [''])[0] if market.get('outcomes') else None,
                    'active': True
                }
                
                if existing.data:
                    # Update existing
                    self.supabase.table('polymarket_tracked_markets')\
                        .update(market_record)\
                        .eq('condition_id', condition_id)\
                        .execute()
                else:
                    # Insert new
                    self.supabase.table('polymarket_tracked_markets')\
                        .insert(market_record)\
                        .execute()
                
                synced_slugs.append(market_slug)
                logger.info(f"Synced market: {market_slug}")
                
            except Exception as e:
                logger.error(f"Error syncing market {market.get('slug')}: {e}")
                continue
        
        logger.info(f"Synced {len(synced_slugs)} markets for event: {event_slug}")
        return synced_slugs
    
    def get_tracked_markets(self) -> List[Dict[str, Any]]:
        """Get list of markets to track from Supabase"""
        try:
            response = self.supabase.table('polymarket_tracked_markets')\
                .select('condition_id, market_slug, event_id')\
                .eq('active', True)\
                .execute()
            
            markets = [{'condition_id': row['condition_id'], 
                       'slug': row['market_slug'],
                       'event_id': row.get('event_id')} 
                      for row in response.data]
            logger.info(f"Found {len(markets)} tracked markets")
            return markets
        except Exception as e:
            logger.error(f"Error fetching tracked markets: {e}")
            return []
    
    def _parse_json_field(self, field: Any) -> Optional[Any]:
        """Parse JSON field if it's a string"""
        if isinstance(field, str):
            try:
                return json.loads(field)
            except json.JSONDecodeError:
                return None
        return field
    
    def _to_float(self, value: Any) -> Optional[float]:
        """Convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _to_int(self, value: Any) -> Optional[int]:
        """Convert value to int"""
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
        """Parse market data into database format"""
        outcome_prices = self._parse_json_field(market.get('outcomePrices'))
        outcomes = self._parse_json_field(market.get('outcomes'))
        clob_token_ids = self._parse_json_field(market.get('clobTokenIds'))
        
        return {
            'condition_id': market.get('conditionId'),
            'market_slug': market.get('slug'),
            'question': market.get('question'),
            'snapshot_timestamp': datetime.now(timezone.utc).isoformat(),
            'active': market.get('active'),
            'closed': market.get('closed'),
            'archived': market.get('archived'),
            'restricted': market.get('restricted'),
            'neg_risk': market.get('negRisk', False),
            'accepting_orders': market.get('acceptingOrders'),
            'volume': self._to_float(market.get('volume')),
            'liquidity': self._to_float(market.get('liquidity')),
            'open_interest': self._to_float(market.get('openInterest')),
            'volume_24hr': self._to_float(market.get('volume24hr')),
            'volume_1wk': self._to_float(market.get('volume1wk')),
            'volume_1mo': self._to_float(market.get('volume1mo')),
            'volume_1yr': self._to_float(market.get('volume1yr')),
            'volume_clob': self._to_float(market.get('volumeClob')),
            'volume_24hr_clob': self._to_float(market.get('volume24hrClob')),
            'volume_1wk_clob': self._to_float(market.get('volume1wkClob')),
            'volume_1mo_clob': self._to_float(market.get('volume1moClob')),
            'volume_1yr_clob': self._to_float(market.get('volume1yrClob')),
            'liquidity_num': self._to_float(market.get('liquidityNum')),
            'liquidity_clob': self._to_float(market.get('liquidityClob')),
            'outcome_prices': outcome_prices,
            'last_trade_price': self._to_float(market.get('lastTradePrice')),
            'best_bid': self._to_float(market.get('bestBid')),
            'best_ask': self._to_float(market.get('bestAsk')),
            'spread': self._to_float(market.get('spread')),
            'one_hour_price_change': self._to_float(market.get('oneHourPriceChange')),
            'one_day_price_change': self._to_float(market.get('oneDayPriceChange')),
            'one_week_price_change': self._to_float(market.get('oneWeekPriceChange')),
            'one_month_price_change': self._to_float(market.get('oneMonthPriceChange')),
            'outcomes': outcomes,
            'clob_token_ids': clob_token_ids,
            'market_type': market.get('marketType'),
            'category': market.get('category'),
            'description': market.get('description'),
            'image_url': market.get('image'),
            'icon_url': market.get('icon'),
            'start_date': self._parse_timestamp(market.get('startDate')),
            'end_date': self._parse_timestamp(market.get('endDate')),
            'accepting_orders_timestamp': self._parse_timestamp(market.get('acceptingOrdersTimestamp')),
            'order_price_min_tick_size': self._to_float(market.get('orderPriceMinTickSize')),
            'order_min_size': self._to_float(market.get('orderMinSize')),
            'rewards_min_size': self._to_float(market.get('rewardsMinSize')),
            'rewards_max_spread': self._to_float(market.get('rewardsMaxSpread')),
            'competitive': self._to_float(market.get('competitive')),
            'comment_count': self._to_int(market.get('commentCount')),
            'uma_bond': self._to_float(market.get('umaBond')),
            'uma_reward': self._to_float(market.get('umaReward')),
            'enable_order_book': market.get('enableOrderBook'),
            'cyom': market.get('cyom'),
            'featured': market.get('featured'),
            'new': market.get('new'),
            'approved': market.get('approved'),
            'updated_at': self._parse_timestamp(market.get('updatedAt'))
        }
    
    def store_snapshot(self, parsed_data: Dict[str, Any]) -> bool:
        """Store market snapshot in Supabase"""
        try:
            self.supabase.table('polymarket_snapshots').insert(parsed_data).execute()
            logger.info(f"Stored snapshot for market: {parsed_data['market_slug']}")
            return True
        except Exception as e:
            logger.error(f"Error storing snapshot: {e}")
            return False
    
    def collect_all(self) -> Dict[str, int]:
        """Main collection function"""
        logger.info("Starting Polymarket data collection")
        
        markets = self.get_tracked_markets()
        
        if not markets:
            logger.warning("No tracked markets found.")
            return {'success': 0, 'failed': 0, 'total': 0}
        
        success_count = 0
        failed_count = 0
        
        for market in markets:
            market_slug = market['slug']
            condition_id = market['condition_id']
            logger.info(f"Processing: {market_slug} (condition_id: {condition_id})")
            
            market_data = self.fetch_market_data(market_slug)
            
            if not market_data:
                failed_count += 1
                continue
            
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