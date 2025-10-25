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
    
    def get_tracked_markets(self) -> List[str]:
        """Get list of condition IDs to track from Supabase"""
        try:
            response = self.supabase.table('polymarket_tracked_markets')\
                .select('condition_id')\
                .eq('active', True)\
                .execute()
            
            condition_ids = [row['condition_id'] for row in response.data]
            logger.info(f"Found {len(condition_ids)} tracked markets")
            return condition_ids
        except Exception as e:
            logger.error(f"Error fetching tracked markets: {e}")
            return []
    
    def fetch_market_data(self, condition_id: str) -> Optional[Dict[str, Any]]:
        """Fetch market data for a specific condition ID"""
        try:
            url = f"{GAMMA_API_BASE}/markets"
            params = {'condition_id': condition_id}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data and len(data) > 0:
                return data[0]  # Returns first matching market
            else:
                logger.warning(f"No data found for condition_id: {condition_id}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Error fetching market {condition_id}: {e}")
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
    
    def parse_market_data(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """Parse market data into database format"""
        # Parse JSON strings if needed
        outcome_prices = market.get('outcomePrices')
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except json.JSONDecodeError:
                outcome_prices = None
        
        clob_token_ids = market.get('clobTokenIds')
        if isinstance(clob_token_ids, str):
            try:
                clob_token_ids = json.loads(clob_token_ids)
            except json.JSONDecodeError:
                clob_token_ids = None
        
        outcomes = market.get('outcomes')
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except json.JSONDecodeError:
                outcomes = None
        
        # Convert volume and liquidity to float
        volume = market.get('volume')
        liquidity = market.get('liquidity')
        
        try:
            volume = float(volume) if volume else None
        except (ValueError, TypeError):
            volume = None
            
        try:
            liquidity = float(liquidity) if liquidity else None
        except (ValueError, TypeError):
            liquidity = None
        
        return {
            'condition_id': market.get('conditionId'),
            'market_slug': market.get('slug'),
            'question': market.get('question'),
            'snapshot_timestamp': datetime.now(timezone.utc).isoformat(),
            'active': market.get('active'),
            'closed': market.get('closed'),
            'archived': market.get('archived'),
            'volume': volume,
            'liquidity': liquidity,
            'outcome_prices': outcome_prices,
            'outcomes': outcomes,
            'clob_token_ids': clob_token_ids,
            'market_type': market.get('marketType'),
            'category': market.get('category'),
            'start_date': market.get('startDate'),
            'end_date': market.get('endDate'),
            'neg_risk': market.get('negRisk', False),
            'description': market.get('description'),
            'image_url': market.get('image')
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
        condition_ids = self.get_tracked_markets()
        
        if not condition_ids:
            logger.warning("No tracked markets found. Add markets to polymarket_tracked_markets table.")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        success_count = 0
        failed_count = 0
        
        for condition_id in condition_ids:
            logger.info(f"Processing market: {condition_id}")
            
            # Fetch market data
            market_data = self.fetch_market_data(condition_id)
            
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
            'total': len(condition_ids)
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
