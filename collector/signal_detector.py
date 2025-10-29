#!/usr/bin/env python3
"""
Market Signal Detector
Detects significant price movements in prediction markets and stores them as signal events
"""
from dotenv import load_dotenv
load_dotenv()
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import json
import csv
from dateutil import parser as date_parser

from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')


class SignalType(Enum):
    """Types of signals that can be detected"""
    ABSOLUTE_CHANGE = "absolute_change"  # Large absolute price change
    RELATIVE_CHANGE = "relative_change"  # Large percentage change
    RAPID_MOVEMENT = "rapid_movement"    # Fast change in short time
    VOLATILITY_SPIKE = "volatility_spike"  # Sudden increase in volatility


class Direction(Enum):
    """Direction of price movement"""
    UP = "up"
    DOWN = "down"


@dataclass
class SignalConfig:
    """Configuration for signal detection thresholds"""
    # Absolute change thresholds (in price points, 0-1 scale)
    min_absolute_change: float = 0.10  # 10 percentage points
    large_absolute_change: float = 0.20  # 20 percentage points
    
    # Relative change thresholds (percentage of current price)
    min_relative_change: float = 0.25  # 25% change
    large_relative_change: float = 0.50  # 50% change
    
    # Time windows for analysis (in minutes)
    short_window: int = 15    # 15 minutes
    medium_window: int = 60   # 1 hour
    long_window: int = 240    # 4 hours
    day_window: int = 1440
    
    # Minimum price to consider (avoid noise from very low probability events)
    min_price_threshold: float = 0.05  # 5%
    
    # Lookback period for historical analysis (in hours)
    historical_lookback: int = 24


def get_supabase_client() -> Client:
    """Initialize and return Supabase client"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


class SignalDetector:
    """Detects significant market signals from price data"""
    
    def __init__(self, config: Optional[SignalConfig] = None):
        self.supabase = get_supabase_client()
        self.config = config or SignalConfig()
    
    def ensure_signals_table(self):
        """Ensure the market_signals table exists"""
        # Note: This should ideally be done via migration, but including SQL for reference
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.market_signals (
            id BIGSERIAL PRIMARY KEY,
            market_id TEXT NOT NULL,
            ticker TEXT,  -- For Kalshi
            condition_id TEXT,  -- For Polymarket
            source TEXT NOT NULL CHECK (source IN ('polymarket', 'kalshi')),
            signal_type TEXT NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            direction TEXT NOT NULL CHECK (direction IN ('up', 'down')),
            prior_price NUMERIC(10, 6) NOT NULL,
            new_price NUMERIC(10, 6) NOT NULL,
            price_change NUMERIC(10, 6) NOT NULL,
            percent_change NUMERIC(10, 4),
            time_window_minutes INTEGER,
            explanation TEXT,
            metadata JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            CONSTRAINT unique_signal UNIQUE (market_id, timestamp, signal_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_signals_market_id ON public.market_signals(market_id);
        CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON public.market_signals(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_signals_source ON public.market_signals(source);
        CREATE INDEX IF NOT EXISTS idx_signals_type ON public.market_signals(signal_type);
        """
        logger.info("Signals table should be created via migration")
        # The actual table creation should be done via Supabase dashboard or migration
    
    def get_polymarket_price_data(
        self, 
        condition_id: str, 
        lookback_hours: int
    ) -> List[Dict[str, Any]]:
        """Get Polymarket price history for a condition"""
        try:
            start_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
            
            response = self.supabase.table('polymarket_price_history')\
                .select('*')\
                .eq('condition_id', condition_id)\
                .gte('timestamp', start_time.isoformat())\
                .order('timestamp', desc=False)\
                .execute()
            
            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error fetching Polymarket data for {condition_id}: {e}")
            return []
    
    def get_all_polymarket_price_data(self, condition_id: str) -> List[Dict[str, Any]]:
        """Get all available Polymarket price history for a condition"""
        try:
            response = self.supabase.table('polymarket_price_history')\
                .select('*')\
                .eq('condition_id', condition_id)\
                .order('timestamp', desc=False)\
                .limit(10000)\
                .execute()
            
            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error fetching all Polymarket data for {condition_id}: {e}")
            return []
    
    def get_kalshi_price_data(
        self, 
        ticker: str, 
        lookback_hours: int
    ) -> List[Dict[str, Any]]:
        """Get Kalshi price history for a ticker"""
        try:
            start_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
            
            response = self.supabase.table('kalshi_price_history')\
                .select('*')\
                .eq('ticker', ticker)\
                .gte('timestamp', start_time.isoformat())\
                .order('timestamp', desc=False)\
                .execute()
            
            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error fetching Kalshi data for {ticker}: {e}")
            return []
    
    def get_all_kalshi_price_data(self, ticker: str) -> List[Dict[str, Any]]:
        """Get all available Kalshi price history for a ticker"""
        try:
            response = self.supabase.table('kalshi_price_history')\
                .select('*')\
                .eq('ticker', ticker)\
                .order('timestamp', desc=False)\
                .limit(10000)\
                .execute()
            
            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error fetching all Kalshi data for {ticker}: {e}")
            return []
    
    def normalize_kalshi_price(self, row: Dict[str, Any]) -> Optional[float]:
        """Extract a single price from Kalshi data (prefer close, fallback to mean)"""
        if row.get('price_close') is not None:
            return float(row['price_close'])
        elif row.get('price_mean') is not None:
            return float(row['price_mean'])
        return None
    
    def detect_signals_in_window(
        self,
        prices: List[Tuple[datetime, float]],
        window_minutes: int,
        market_id: str,
        source: str
    ) -> List[Dict[str, Any]]:
        """Detect signals within a specific time window"""
        signals = []
        
        if len(prices) < 2:
            return signals
        
        # Track when we last detected a signal to avoid duplicates
        last_signal_time = None
        cooldown_minutes = window_minutes  # Don't detect another signal within the same window
        
        # Analyze price changes over the window
        for i in range(1, len(prices)):
            current_time, current_price = prices[i]
            
            # Skip if we're in cooldown period from last signal
            if last_signal_time and (current_time - last_signal_time).total_seconds() < cooldown_minutes * 60:
                continue
            
            # Find price at window_minutes ago
            window_start = current_time - timedelta(minutes=window_minutes)
            
            # Find the closest price to window_start
            prior_price = None
            prior_time = None
            
            for j in range(i - 1, -1, -1):
                check_time, check_price = prices[j]
                if check_time <= window_start:
                    prior_price = check_price
                    prior_time = check_time
                    break
            
            if prior_price is None or prior_price < self.config.min_price_threshold:
                continue
            
            # Calculate changes
            absolute_change = abs(current_price - prior_price)
            relative_change = absolute_change / prior_price if prior_price > 0 else 0
            direction = Direction.UP if current_price > prior_price else Direction.DOWN
            
            # Check thresholds
            detected = False
            signal_type = None
            explanation = None
            
            # Absolute change detection
            if absolute_change >= self.config.large_absolute_change:
                detected = True
                signal_type = SignalType.ABSOLUTE_CHANGE.value
                explanation = f"Large absolute change of {absolute_change:.1%} ({direction.value}) in {window_minutes} minutes"
            elif absolute_change >= self.config.min_absolute_change and window_minutes <= self.config.short_window:
                detected = True
                signal_type = SignalType.RAPID_MOVEMENT.value
                explanation = f"Rapid {direction.value} movement of {absolute_change:.1%} in {window_minutes} minutes"
            
            # Relative change detection
            if relative_change >= self.config.large_relative_change:
                detected = True
                signal_type = SignalType.RELATIVE_CHANGE.value
                explanation = f"Large relative change of {relative_change:.1%} ({direction.value}) in {window_minutes} minutes"
            elif relative_change >= self.config.min_relative_change and window_minutes <= self.config.medium_window:
                if not detected:  # Don't override absolute change signals
                    detected = True
                    signal_type = SignalType.RELATIVE_CHANGE.value
                    explanation = f"Significant relative change of {relative_change:.1%} ({direction.value}) in {window_minutes} minutes"
            
            if detected:
                signals.append({
                    'market_id': market_id,
                    'source': source,
                    'signal_type': signal_type,
                    'timestamp': current_time.isoformat(),
                    'direction': direction.value,
                    'prior_price': float(prior_price),
                    'new_price': float(current_price),
                    'price_change': float(current_price - prior_price),
                    'percent_change': float(relative_change),
                    'time_window_minutes': window_minutes,
                    'explanation': explanation,
                    'metadata': {
                        'prior_timestamp': prior_time.isoformat() if prior_time else None,
                        'absolute_change': float(absolute_change),
                        'config_used': {
                            'min_absolute': self.config.min_absolute_change,
                            'min_relative': self.config.min_relative_change
                        }
                    },
                    # Store condition_id for Polymarket, will be set properly in store_signals
                    'condition_id': market_id if source == 'polymarket' else None,
                    'ticker': market_id if source == 'kalshi' else None
                })
                
                # Set cooldown to avoid detecting the same movement multiple times
                last_signal_time = current_time
        
        return signals
    
    def detect_polymarket_signals(
        self,
        condition_id: str,
        lookback_hours: Optional[int] = None,
        use_all_available: bool = False
    ) -> List[Dict[str, Any]]:
        """Detect signals for a Polymarket market"""
        lookback = lookback_hours or self.config.historical_lookback
        
        logger.info(f"Analyzing Polymarket condition: {condition_id}")
        
        # Get price data
        price_data = self.get_polymarket_price_data(condition_id, lookback)
        
        # If no recent data and use_all_available is True, try to get any data
        if not price_data and use_all_available:
            logger.info(f"No recent data, fetching all available data for {condition_id}")
            price_data = self.get_all_polymarket_price_data(condition_id)
        
        if not price_data:
            logger.warning(f"No price data for {condition_id}")
            return []
        
        # Normalize to (timestamp, price) tuples
        prices = []
        for row in price_data:
            if row.get('price') is not None:
                timestamp = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                prices.append((timestamp, float(row['price'])))
        
        if len(prices) < 2:
            logger.warning(f"Insufficient price data for {condition_id}")
            return []
        
        logger.info(f"Analyzing {len(prices)} price points for {condition_id}")
        
        # Detect signals across different time windows
        all_signals = []
        
        for window in [self.config.short_window, self.config.medium_window, self.config.long_window, self.config.day_window]:
            signals = self.detect_signals_in_window(
                prices, window, condition_id, 'polymarket'
            )
            all_signals.extend(signals)
        
        # Deduplicate signals (keep most significant per timestamp)
        unique_signals = self.deduplicate_signals(all_signals)
        
        logger.info(f"Detected {len(unique_signals)} signals for {condition_id}")
        return unique_signals
    
    def detect_kalshi_signals(
        self,
        ticker: str,
        lookback_hours: Optional[int] = None,
        use_all_available: bool = False
    ) -> List[Dict[str, Any]]:
        """Detect signals for a Kalshi market"""
        lookback = lookback_hours or self.config.historical_lookback
        
        logger.info(f"Analyzing Kalshi ticker: {ticker}")
        
        # Get price data
        price_data = self.get_kalshi_price_data(ticker, lookback)
        
        # If no recent data and use_all_available is True, try to get any data
        if not price_data and use_all_available:
            logger.info(f"No recent data, fetching all available data for {ticker}")
            price_data = self.get_all_kalshi_price_data(ticker)
        
        if not price_data:
            logger.warning(f"No price data for {ticker}")
            return []
        
        # Normalize to (timestamp, price) tuples
        prices = []
        for row in price_data:
            price = self.normalize_kalshi_price(row)
            if price is not None:
                timestamp = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                prices.append((timestamp, price))
        
        if len(prices) < 2:
            logger.warning(f"Insufficient price data for {ticker}")
            return []
        
        logger.info(f"Analyzing {len(prices)} price points for {ticker}")
        
        # Detect signals across different time windows
        all_signals = []
        
        for window in [self.config.short_window, self.config.medium_window, self.config.long_window, self.config.day_window]:
            signals = self.detect_signals_in_window(
                prices, window, ticker, 'kalshi'
            )
            all_signals.extend(signals)
        
        # Add ticker to metadata
        for signal in all_signals:
            signal['ticker'] = ticker
        
        # Deduplicate signals
        unique_signals = self.deduplicate_signals(all_signals)
        
        logger.info(f"Detected {len(unique_signals)} signals for {ticker}")
        return unique_signals
    
    def deduplicate_signals(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate signals, keeping only the first detection of each price movement
        
        Strategy:
        1. Group signals by time proximity (within 5 minutes = same event)
        2. Within each group, keep only the earliest detection
        3. If multiple time windows detected the same event, prefer shorter windows (more immediate)
        """
        if not signals:
            return []
        
        # Sort by timestamp
        sorted_signals = sorted(signals, key=lambda x: x['timestamp'])
        
        unique = []
        last_kept_signal = None
        
        for signal in sorted_signals:
            current_time = datetime.fromisoformat(signal['timestamp'])
            
            # If this is the first signal, keep it
            if last_kept_signal is None:
                unique.append(signal)
                last_kept_signal = signal
                continue
            
            last_time = datetime.fromisoformat(last_kept_signal['timestamp'])
            time_diff_minutes = (current_time - last_time).total_seconds() / 60
            
            # Check if this is the same event (within 5 minutes and same direction)
            if (time_diff_minutes <= 5 and 
                signal['direction'] == last_kept_signal['direction']):
                
                # Same event detected multiple times - only keep if:
                # 1. It's a different, more significant type of signal
                # 2. The price change is substantially larger (>20% more)
                
                price_change_diff = abs(signal['price_change']) - abs(last_kept_signal['price_change'])
                relative_increase = price_change_diff / abs(last_kept_signal['price_change']) if last_kept_signal['price_change'] != 0 else 0
                
                if relative_increase > 0.2:  # 20% larger movement
                    # Replace the last signal with this larger one
                    unique[-1] = signal
                    last_kept_signal = signal
                # Otherwise skip this signal (it's a duplicate)
            else:
                # Different event - keep it
                unique.append(signal)
                last_kept_signal = signal
        
        return unique
    
    def store_signals(self, signals: List[Dict[str, Any]]) -> int:
        """Store detected signals in the database
        
        Uses upsert to avoid duplicates - signals with same market_id, timestamp, 
        and signal_type will be updated rather than duplicated
        """
        if not signals:
            return 0
        
        try:
            # Prepare records with consistent fields
            prepared_signals = []
            for signal in signals:
                # Create a clean record with all required fields
                record = {
                    'market_id': signal['market_id'],
                    'source': signal['source'],
                    'signal_type': signal['signal_type'],
                    'timestamp': signal['timestamp'],
                    'direction': signal['direction'],
                    'prior_price': signal['prior_price'],
                    'new_price': signal['new_price'],
                    'price_change': signal['price_change'],
                    'percent_change': signal.get('percent_change'),
                    'time_window_minutes': signal.get('time_window_minutes'),
                    'explanation': signal.get('explanation'),
                    'metadata': signal.get('metadata', {}),  # Keep as dict, Supabase will handle JSONB
                    'ticker': signal.get('ticker'),  # For Kalshi
                    'condition_id': signal.get('condition_id')  # For Polymarket
                }
                prepared_signals.append(record)
            
            # Use upsert with onConflict to handle duplicates
            # The unique constraint is on (market_id, timestamp, signal_type)
            response = self.supabase.table('market_signals')\
                .upsert(prepared_signals, on_conflict='market_id,timestamp,signal_type')\
                .execute()
            
            count = len(response.data) if response.data else 0
            logger.info(f"Stored/updated {count} signals")
            return count
            
        except Exception as e:
            logger.error(f"Error storing signals: {e}")
            return 0
    
    def get_active_polymarket_markets(self) -> List[str]:
        """Get list of active Polymarket condition IDs"""
        try:
            response = self.supabase.table('polymarket_tracked_markets')\
                .select('condition_id')\
                .eq('active', True)\
                .execute()
            
            return [m['condition_id'] for m in response.data] if response.data else []
        except Exception as e:
            logger.error(f"Error fetching active Polymarket markets: {e}")
            return []
    
    def get_active_kalshi_markets(self) -> List[str]:
        """Get list of active Kalshi tickers"""
        try:
            # Get unique tickers from recent price history
            response = self.supabase.table('kalshi_price_history')\
                .select('ticker')\
                .gte('timestamp', (datetime.now(timezone.utc) - timedelta(days=7)).isoformat())\
                .execute()
            
            if response.data:
                tickers = list(set([row['ticker'] for row in response.data]))
                return tickers
            return []
        except Exception as e:
            logger.error(f"Error fetching active Kalshi markets: {e}")
            return []
    
    def run_detection_all_markets(
        self, 
        lookback_hours: Optional[int] = None,
        use_all_available: bool = False
    ) -> Dict[str, Any]:
        """Run signal detection on all active markets
        
        Args:
            lookback_hours: Hours to look back (default from config)
            use_all_available: If True, use all available data when no recent data exists
        """
        logger.info("Starting signal detection for all markets")
        
        all_signals = []
        stats = {
            'polymarket': {'markets': 0, 'signals': 0},
            'kalshi': {'markets': 0, 'signals': 0}
        }
        
        # Process Polymarket markets
        poly_markets = self.get_active_polymarket_markets()
        logger.info(f"Found {len(poly_markets)} Polymarket markets")
        
        for condition_id in poly_markets:
            try:
                signals = self.detect_polymarket_signals(condition_id, lookback_hours, use_all_available)
                all_signals.extend(signals)
                stats['polymarket']['markets'] += 1
                stats['polymarket']['signals'] += len(signals)
            except Exception as e:
                logger.error(f"Error processing Polymarket {condition_id}: {e}")
        
        # Process Kalshi markets
        kalshi_tickers = self.get_active_kalshi_markets()
        logger.info(f"Found {len(kalshi_tickers)} Kalshi tickers")
        
        for ticker in kalshi_tickers:
            try:
                signals = self.detect_kalshi_signals(ticker, lookback_hours, use_all_available)
                all_signals.extend(signals)
                stats['kalshi']['markets'] += 1
                stats['kalshi']['signals'] += len(signals)
            except Exception as e:
                logger.error(f"Error processing Kalshi {ticker}: {e}")
        
        # Store all signals
        stored_count = self.store_signals(all_signals)
        
        return {
            'status': 'success',
            'total_signals_detected': len(all_signals),
            'total_signals_stored': stored_count,
            'stats': stats,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


    def detect_signals_from_csv(
        self,
        csv_path: str,
        market_id: str,
        source: str,
        price_column: str = 'price',
        timestamp_column: str = 'timestamp'
    ) -> List[Dict[str, Any]]:
        """Detect signals from a CSV file
        
        Args:
            csv_path: Path to CSV file
            market_id: Market identifier (condition_id for Polymarket, ticker for Kalshi)
            source: 'polymarket' or 'kalshi'
            price_column: Name of the price column in CSV
            timestamp_column: Name of the timestamp column in CSV
        
        Returns:
            List of detected signals
        """
        logger.info(f"Analyzing CSV file: {csv_path}")
        
        # Read CSV and extract price data
        prices = []
        
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                
                # Detect column names (case-insensitive)
                if reader.fieldnames:
                    field_map = {k.lower(): k for k in reader.fieldnames}
                    
                    # Find price column
                    price_col = None
                    for candidate in [price_column.lower(), 'price', 'price_close', 'close']:
                        if candidate in field_map:
                            price_col = field_map[candidate]
                            break
                    
                    # Find timestamp column
                    time_col = None
                    for candidate in [timestamp_column.lower(), 'timestamp', 'datetime', 'time', 'date']:
                        if candidate in field_map:
                            time_col = field_map[candidate]
                            break
                    
                    if not price_col or not time_col:
                        logger.error(f"Could not find required columns. Available: {reader.fieldnames}")
                        return []
                    
                    logger.info(f"Using columns: timestamp={time_col}, price={price_col}")
                    
                    for row in reader:
                        try:
                            timestamp_str = row[time_col]
                            price_str = row[price_col]
                            
                            # Parse timestamp
                            timestamp = date_parser.parse(timestamp_str)
                            if timestamp.tzinfo is None:
                                timestamp = timestamp.replace(tzinfo=timezone.utc)
                            
                            # Parse price (handle different formats)
                            price = float(price_str)
                            
                            # For Kalshi, prices are in cents, convert to 0-1 scale
                            if source == 'kalshi' and price > 1:
                                price = price / 100.0
                            
                            prices.append((timestamp, price))
                        except (ValueError, KeyError) as e:
                            logger.warning(f"Skipping invalid row: {e}")
                            continue
            
            if len(prices) < 2:
                logger.warning(f"Insufficient price data in CSV ({len(prices)} rows)")
                return []
            
            # Sort by timestamp
            prices.sort(key=lambda x: x[0])
            
            logger.info(f"Loaded {len(prices)} price points from CSV")
            
            # Detect signals across different time windows
            all_signals = []
            
            for window in [self.config.short_window, self.config.medium_window, self.config.long_window, self.config.day_window]:
                signals = self.detect_signals_in_window(
                    prices, window, market_id, source
                )
                all_signals.extend(signals)
            
            # Add appropriate ID field
            for signal in all_signals:
                if source == 'kalshi':
                    signal['ticker'] = market_id
                    signal['condition_id'] = None
                else:  # polymarket
                    signal['condition_id'] = market_id
                    signal['ticker'] = None
            
            # Deduplicate signals
            unique_signals = self.deduplicate_signals(all_signals)
            
            logger.info(f"Detected {len(unique_signals)} signals from CSV")
            return unique_signals
            
        except Exception as e:
            logger.error(f"Error processing CSV {csv_path}: {e}", exc_info=True)
            return []


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Detect market signals')
    parser.add_argument('--lookback-hours', type=int, default=24,
                       help='Hours of historical data to analyze')
    parser.add_argument('--min-absolute', type=float, default=0.10,
                       help='Minimum absolute change threshold')
    parser.add_argument('--min-relative', type=float, default=0.25,
                       help='Minimum relative change threshold')
    parser.add_argument('--csv', type=str,
                       help='Path to CSV file to analyze')
    parser.add_argument('--market-id', type=str,
                       help='Market ID (required with --csv)')
    parser.add_argument('--source', type=str, choices=['polymarket', 'kalshi'],
                       help='Source platform (required with --csv)')
    parser.add_argument('--price-column', type=str, default='price',
                       help='Name of price column in CSV')
    parser.add_argument('--timestamp-column', type=str, default='timestamp',
                       help='Name of timestamp column in CSV')
    
    args = parser.parse_args()
    
    # Create config
    config = SignalConfig(
        min_absolute_change=args.min_absolute,
        min_relative_change=args.min_relative,
        historical_lookback=args.lookback_hours
    )
    
    detector = SignalDetector(config)
    
    # CSV mode
    if args.csv:
        if not args.market_id or not args.source:
            parser.error("--market-id and --source are required when using --csv")
        
        signals = detector.detect_signals_from_csv(
            args.csv,
            args.market_id,
            args.source,
            args.price_column,
            args.timestamp_column
        )
        
        # Store signals
        stored = detector.store_signals(signals)
        
        results = {
            'status': 'success',
            'csv_file': args.csv,
            'market_id': args.market_id,
            'source': args.source,
            'signals_detected': len(signals),
            'signals_stored': stored,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        print(json.dumps(results, indent=2))
    
    # Database mode
    else:
        results = detector.run_detection_all_markets(args.lookback_hours)
        print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()