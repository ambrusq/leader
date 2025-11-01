#!/usr/bin/env python3
"""
Market Signal Detector with Trend Detection
Detects both short-term alerts (relative changes) and sustained trends
"""
from dotenv import load_dotenv
load_dotenv()

import os
import logging
import csv
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')


@dataclass
class Signal:
    """Signal data structure for both alerts and trends"""
    market_id: str
    source: str
    timestamp: datetime
    prior_timestamp: datetime
    prior_price: float
    new_price: float
    percent_change: float
    direction: str
    signal_type: str = 'relative_change'  # 'relative_change' or 'trend'
    ticker: Optional[str] = None
    condition_id: Optional[str] = None
    window_size: Optional[int] = None  # For trends


class SignalDetector:
    """Detects signals by comparing neighboring price datapoints and trends"""
    
    def __init__(
        self,
        threshold_percent: float = 0.5,
        trend_threshold_percent: float = 0.15,
        trend_window_size: int = 10,
        trend_stability_points: int = 3
    ):
        """
        Initialize detector
        
        Args:
            threshold_percent: Minimum relative change for alerts (default 5%)
            trend_threshold_percent: Minimum change for trends (default 15%)
            trend_window_size: Number of points for rolling window baseline (default 10)
            trend_stability_points: Points to confirm trend isn't reversed (default 3)
        """
        self.threshold = threshold_percent
        self.trend_threshold = trend_threshold_percent
        self.trend_window_size = trend_window_size
        self.trend_stability_points = trend_stability_points
        self.supabase = self._get_supabase_client()
    
    def _get_supabase_client(self) -> Client:
        """Initialize Supabase client"""
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    
    def detect_signals(self, prices: List[Tuple[datetime, float]]) -> List[Signal]:
        """
        Detect alert signals from a list of (timestamp, price) tuples
        
        Args:
            prices: List of (timestamp, price) sorted by timestamp
            
        Returns:
            List of detected signals
        """
        signals = []
        
        if len(prices) < 2:
            return signals
        
        for i in range(1, len(prices)):
            prior_time, prior_price = prices[i-1]
            current_time, current_price = prices[i]
            
            # Skip if prior price is zero (avoid division by zero)
            if prior_price == 0:
                continue
            
            # Calculate relative change
            price_change = current_price - prior_price
            percent_change = price_change / prior_price
            
            # Check if change exceeds threshold
            if abs(percent_change) >= self.threshold:
                direction = 'up' if price_change > 0 else 'down'
                
                signal = Signal(
                    market_id='',  # Will be set by caller
                    source='',     # Will be set by caller
                    timestamp=current_time,
                    prior_timestamp=prior_time,
                    prior_price=prior_price,
                    new_price=current_price,
                    percent_change=percent_change,
                    direction=direction,
                    signal_type='relative_change'
                )
                signals.append(signal)
        
        return signals
    
    def detect_trends(self, prices: List[Tuple[datetime, float]]) -> List[Signal]:
        """
        Detect sustained trend signals using rolling window
        
        Args:
            prices: List of (timestamp, price) sorted by timestamp
            
        Returns:
            List of detected trend signals
        """
        trends = []
        
        # Need enough points for window + stability check
        min_points = self.trend_window_size + self.trend_stability_points
        if len(prices) < min_points:
            return trends
        
        # Track last detected trend to avoid duplicates for same trend
        last_trend_idx = -1
        
        for i in range(self.trend_window_size, len(prices) - self.trend_stability_points + 1):
            # Get window of previous prices
            window_start = i - self.trend_window_size
            window = prices[window_start:i]
            window_prices = [p for _, p in window]
            
            # Calculate window baseline (mean)
            window_mean = sum(window_prices) / len(window_prices)
            
            if window_mean == 0:
                continue
            
            # Current price and timestamp
            current_time, current_price = prices[i]
            window_start_time, window_start_price = window[0]
            
            # Calculate change from window mean
            price_change = current_price - window_mean
            percent_change = price_change / window_mean
            
            # Check if exceeds trend threshold
            if abs(percent_change) < self.trend_threshold:
                continue
            
            direction = 'up' if price_change > 0 else 'down'
            
            # Check stability: verify trend holds for next few points
            is_stable = True
            for j in range(1, min(self.trend_stability_points + 1, len(prices) - i)):
                future_time, future_price = prices[i + j]
                future_change = (future_price - window_mean) / window_mean
                
                # If direction reverses significantly, trend is not stable
                if direction == 'up' and future_change < percent_change * 0.5:
                    is_stable = False
                    break
                elif direction == 'down' and future_change > percent_change * 0.5:
                    is_stable = False
                    break
            
            if not is_stable:
                continue
            
            # Avoid detecting same trend multiple times (skip nearby indices)
            if i - last_trend_idx < self.trend_window_size // 2:
                continue
            
            last_trend_idx = i
            
            trend = Signal(
                market_id='',  # Will be set by caller
                source='',     # Will be set by caller
                timestamp=current_time,
                prior_timestamp=window_start_time,
                prior_price=window_mean,  # Use window mean as baseline
                new_price=current_price,
                percent_change=percent_change,
                direction=direction,
                signal_type='trend',
                window_size=self.trend_window_size
            )
            trends.append(trend)
        
        return trends
    
    def get_polymarket_prices(self, condition_id: str) -> List[Tuple[datetime, float]]:
        """Get all price data for a Polymarket condition"""
        try:
            # Fetch all data using pagination
            all_data = []
            offset = 0
            limit = 1000
            
            while True:
                response = self.supabase.table('polymarket_price_history')\
                    .select('timestamp, price')\
                    .eq('condition_id', condition_id)\
                    .not_.is_('price', 'null')\
                    .order('timestamp', desc=False)\
                    .limit(limit)\
                    .offset(offset)\
                    .execute()
                
                if not response.data:
                    break
                
                all_data.extend(response.data)
                
                # If we got fewer rows than limit, we've reached the end
                if len(response.data) < limit:
                    break
                
                offset += limit
            
            prices = []
            for row in all_data:
                timestamp = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                price = float(row['price'])
                prices.append((timestamp, price))
            
            logger.info(f"Fetched {len(prices)} total price points for {condition_id}")
            return prices
            
        except Exception as e:
            logger.error(f"Error fetching Polymarket prices for {condition_id}: {e}")
            return []
    
    def get_kalshi_prices(self, ticker: str) -> List[Tuple[datetime, float]]:
        """Get all price data for a Kalshi ticker"""
        try:
            # Fetch all data using pagination
            all_data = []
            offset = 0
            limit = 1000
            
            while True:
                response = self.supabase.table('kalshi_price_history')\
                    .select('timestamp, price_close, price_mean')\
                    .eq('ticker', ticker)\
                    .order('timestamp', desc=False)\
                    .limit(limit)\
                    .offset(offset)\
                    .execute()
                
                if not response.data:
                    break
                
                all_data.extend(response.data)
                
                # If we got fewer rows than limit, we've reached the end
                if len(response.data) < limit:
                    break
                
                offset += limit
            
            prices = []
            for row in all_data:
                timestamp = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                
                # Use price_close, fall back to price_mean
                price = row.get('price_close') or row.get('price_mean')
                if price is None:
                    continue
                
                price = float(price)
                
                # Convert from cents to 0-1 scale if needed
                if price > 1:
                    price = price / 100.0
                
                prices.append((timestamp, price))
            
            logger.info(f"Fetched {len(prices)} total price points for {ticker}")
            return prices
            
        except Exception as e:
            logger.error(f"Error fetching Kalshi prices for {ticker}: {e}")
            return []
    
    def process_polymarket_market(
        self,
        condition_id: str,
        detect_trends: bool = True
    ) -> Dict[str, List[Signal]]:
        """
        Process a single Polymarket market
        
        Args:
            condition_id: Market condition ID
            detect_trends: Whether to detect trends (default True)
            
        Returns:
            Dict with 'alerts' and 'trends' lists
        """
        logger.info(f"Processing Polymarket condition: {condition_id}")
        
        prices = self.get_polymarket_prices(condition_id)
        if not prices:
            logger.warning(f"No prices found for {condition_id}")
            return {'alerts': [], 'trends': []}
        
        logger.info(f"Found {len(prices)} price points")
        
        # Detect alerts (relative changes)
        alerts = self.detect_signals(prices)
        for signal in alerts:
            signal.market_id = condition_id
            signal.source = 'polymarket'
            signal.condition_id = condition_id
        
        # Detect trends
        trends = []
        if detect_trends:
            trends = self.detect_trends(prices)
            for signal in trends:
                signal.market_id = condition_id
                signal.source = 'polymarket'
                signal.condition_id = condition_id
        
        logger.info(f"Detected {len(alerts)} alerts and {len(trends)} trends")
        return {'alerts': alerts, 'trends': trends}
    
    def process_kalshi_market(
        self,
        ticker: str,
        detect_trends: bool = True
    ) -> Dict[str, List[Signal]]:
        """
        Process a single Kalshi market
        
        Args:
            ticker: Market ticker
            detect_trends: Whether to detect trends (default True)
            
        Returns:
            Dict with 'alerts' and 'trends' lists
        """
        logger.info(f"Processing Kalshi ticker: {ticker}")
        
        prices = self.get_kalshi_prices(ticker)
        if not prices:
            logger.warning(f"No prices found for {ticker}")
            return {'alerts': [], 'trends': []}
        
        logger.info(f"Found {len(prices)} price points")
        
        # Detect alerts (relative changes)
        alerts = self.detect_signals(prices)
        for signal in alerts:
            signal.market_id = ticker
            signal.source = 'kalshi'
            signal.ticker = ticker
        
        # Detect trends
        trends = []
        if detect_trends:
            trends = self.detect_trends(prices)
            for signal in trends:
                signal.market_id = ticker
                signal.source = 'kalshi'
                signal.ticker = ticker
        
        logger.info(f"Detected {len(alerts)} alerts and {len(trends)} trends")
        return {'alerts': alerts, 'trends': trends}
    
    def get_active_polymarket_conditions(self) -> List[str]:
        """Get list of active Polymarket condition IDs"""
        try:
            response = self.supabase.table('polymarket_tracked_markets')\
                .select('condition_id')\
                .eq('active', True)\
                .execute()
            
            return [m['condition_id'] for m in response.data] if response.data else []
        except Exception as e:
            logger.error(f"Error fetching Polymarket markets: {e}")
            return []
    
    def get_active_kalshi_tickers(self) -> List[str]:
        """Get list of active Kalshi tickers"""
        try:
            response = self.supabase.table('kalshi_tracked_markets')\
                .select('ticker')\
                .eq('active', True)\
                .execute()
            
            return [m['ticker'] for m in response.data] if response.data else []
        except Exception as e:
            logger.error(f"Error fetching Kalshi markets: {e}")
            return []
    
    def process_all_markets(self, detect_trends: bool = True) -> Dict[str, Any]:
        """
        Process all active markets from both platforms
        
        Args:
            detect_trends: Whether to detect trends in addition to alerts
            
        Returns:
            Processing results and statistics
        """
        logger.info("Processing all active markets")
        
        all_signals = []
        stats = {
            'polymarket': {'markets': 0, 'alerts': 0, 'trends': 0},
            'kalshi': {'markets': 0, 'alerts': 0, 'trends': 0}
        }
        
        # Process Polymarket
        poly_conditions = self.get_active_polymarket_conditions()
        logger.info(f"Found {len(poly_conditions)} Polymarket markets")
        
        for condition_id in poly_conditions:
            try:
                results = self.process_polymarket_market(condition_id, detect_trends)
                all_signals.extend(results['alerts'])
                all_signals.extend(results['trends'])
                stats['polymarket']['markets'] += 1
                stats['polymarket']['alerts'] += len(results['alerts'])
                stats['polymarket']['trends'] += len(results['trends'])
            except Exception as e:
                logger.error(f"Error processing {condition_id}: {e}")
        
        # Process Kalshi
        kalshi_tickers = self.get_active_kalshi_tickers()
        logger.info(f"Found {len(kalshi_tickers)} Kalshi markets")
        
        for ticker in kalshi_tickers:
            try:
                results = self.process_kalshi_market(ticker, detect_trends)
                all_signals.extend(results['alerts'])
                all_signals.extend(results['trends'])
                stats['kalshi']['markets'] += 1
                stats['kalshi']['alerts'] += len(results['alerts'])
                stats['kalshi']['trends'] += len(results['trends'])
            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
        
        # Store signals
        stored_count = self.store_signals(all_signals)
        
        return {
            'total_signals': len(all_signals),
            'stored_signals': stored_count,
            'stats': stats,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def store_signals(self, signals: List[Signal]) -> int:
        """Store signals in Supabase"""
        if not signals:
            return 0
        
        try:
            records = []
            for signal in signals:
                metadata = {
                    'prior_timestamp': signal.prior_timestamp.isoformat(),
                    'threshold': self.threshold if signal.signal_type == 'relative_change' else self.trend_threshold
                }
                
                # Add trend-specific metadata
                if signal.signal_type == 'trend':
                    metadata['window_size'] = signal.window_size
                    metadata['stability_points'] = self.trend_stability_points
                
                explanation = f"{signal.direction.capitalize()} {abs(signal.percent_change):.1%} change"
                if signal.signal_type == 'trend':
                    explanation = f"Sustained {signal.direction} trend: {abs(signal.percent_change):.1%} from {signal.window_size}-point baseline"
                
                record = {
                    'market_id': signal.market_id,
                    'source': signal.source,
                    'signal_type': signal.signal_type,
                    'timestamp': signal.timestamp.isoformat(),
                    'direction': signal.direction,
                    'prior_price': float(signal.prior_price),
                    'new_price': float(signal.new_price),
                    'price_change': float(signal.new_price - signal.prior_price),
                    'percent_change': float(signal.percent_change),
                    'time_window_minutes': int((signal.timestamp - signal.prior_timestamp).total_seconds() / 60),
                    'explanation': explanation,
                    'metadata': metadata,
                    'ticker': signal.ticker,
                    'condition_id': signal.condition_id
                }
                records.append(record)
            
            response = self.supabase.table('market_signals')\
                .upsert(records, on_conflict='market_id,timestamp,signal_type')\
                .execute()
            
            count = len(response.data) if response.data else 0
            logger.info(f"Stored {count} signals")
            return count
            
        except Exception as e:
            logger.error(f"Error storing signals: {e}")
            return 0
    
    def process_csv(
        self,
        csv_path: str,
        market_id: str,
        source: str,
        output_path: Optional[str] = None,
        detect_trends: bool = True
    ) -> Dict[str, List[Signal]]:
        """
        Process a CSV file and optionally save signals to a CSV
        
        Args:
            csv_path: Path to input CSV
            market_id: Market identifier (ticker or condition_id)
            source: 'polymarket' or 'kalshi'
            output_path: Optional path for output CSV (if None, generates one)
            detect_trends: Whether to detect trends in addition to alerts
        
        Returns:
            Dict with 'alerts' and 'trends' lists
        """
        logger.info(f"Processing CSV: {csv_path}")
        
        # Read CSV
        prices = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            headers = [h.strip().lower() for h in reader.fieldnames]
            
            # Find timestamp and price columns
            time_col = None
            price_col = None
            
            for col in headers:
                if col in ['timestamp', 'datetime', 'time', 'date']:
                    time_col = reader.fieldnames[headers.index(col)]
                if col in ['price', 'price_close', 'close']:
                    price_col = reader.fieldnames[headers.index(col)]
            
            if not time_col or not price_col:
                raise ValueError(f"Could not find timestamp or price columns. Found: {reader.fieldnames}")
            
            logger.info(f"Using columns: {time_col}, {price_col}")
            
            for row in reader:
                try:
                    timestamp = datetime.fromisoformat(row[time_col].replace('Z', '+00:00'))
                    price = float(row[price_col])
                    
                    # Convert Kalshi prices if needed
                    if source == 'kalshi' and price > 1:
                        price = price / 100.0
                    
                    prices.append((timestamp, price))
                except (ValueError, KeyError) as e:
                    logger.warning(f"Skipping row: {e}")
                    continue
        
        if len(prices) < 2:
            logger.error("Insufficient data in CSV")
            return {'alerts': [], 'trends': []}
        
        # Sort by timestamp
        prices.sort(key=lambda x: x[0])
        logger.info(f"Loaded {len(prices)} price points")
        
        # Detect alerts
        alerts = self.detect_signals(prices)
        for signal in alerts:
            signal.market_id = market_id
            signal.source = source
            if source == 'kalshi':
                signal.ticker = market_id
            else:
                signal.condition_id = market_id
        
        # Detect trends
        trends = []
        if detect_trends:
            trends = self.detect_trends(prices)
            for signal in trends:
                signal.market_id = market_id
                signal.source = source
                if source == 'kalshi':
                    signal.ticker = market_id
                else:
                    signal.condition_id = market_id
        
        logger.info(f"Detected {len(alerts)} alerts and {len(trends)} trends")
        
        # Write output CSV
        if output_path is None:
            output_path = csv_path.replace('.csv', '_signals.csv')
        
        all_signals = alerts + trends
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'market_id', 'source', 'signal_type', 'timestamp',
                'direction', 'prior_price', 'new_price', 'price_change',
                'percent_change', 'time_window_minutes', 'explanation',
                'prior_timestamp', 'ticker', 'condition_id', 'window_size'
            ])
            
            for signal in all_signals:
                writer.writerow([
                    signal.market_id,
                    signal.source,
                    signal.signal_type,
                    signal.timestamp.isoformat(),
                    signal.direction,
                    f"{signal.prior_price:.6f}",
                    f"{signal.new_price:.6f}",
                    f"{signal.new_price - signal.prior_price:.6f}",
                    f"{signal.percent_change:.4f}",
                    int((signal.timestamp - signal.prior_timestamp).total_seconds() / 60),
                    signal.explanation if hasattr(signal, 'explanation') else f"{signal.direction.capitalize()} {abs(signal.percent_change):.1%}",
                    signal.prior_timestamp.isoformat(),
                    signal.ticker or '',
                    signal.condition_id or '',
                    signal.window_size or ''
                ])
        
        logger.info(f"Saved signals to {output_path}")
        return {'alerts': alerts, 'trends': trends}


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Detect market signals and trends from price changes')
    parser.add_argument('--threshold', type=float, default=0.5,
                       help='Relative change threshold for alerts (default: 0.05 = 5%%)')
    parser.add_argument('--trend-threshold', type=float, default=0.15,
                       help='Threshold for trend detection (default: 0.15 = 15%%)')
    parser.add_argument('--trend-window', type=int, default=10,
                       help='Window size for trend baseline (default: 10)')
    parser.add_argument('--trend-stability', type=int, default=3,
                       help='Stability points for trend confirmation (default: 3)')
    parser.add_argument('--no-trends', action='store_true',
                       help='Disable trend detection (alerts only)')
    parser.add_argument('--csv', type=str,
                       help='Path to CSV file')
    parser.add_argument('--market-id', type=str,
                       help='Market ID (required with --csv)')
    parser.add_argument('--source', type=str, choices=['polymarket', 'kalshi'],
                       help='Source platform (required with --csv)')
    parser.add_argument('--output', type=str,
                       help='Output CSV path (for --csv mode)')
    
    args = parser.parse_args()
    
    detector = SignalDetector(
        threshold_percent=args.threshold,
        trend_threshold_percent=args.trend_threshold,
        trend_window_size=args.trend_window,
        trend_stability_points=args.trend_stability
    )
    
    detect_trends = not args.no_trends
    
    if args.csv:
        if not args.market_id or not args.source:
            parser.error("--market-id and --source required with --csv")
        
        results = detector.process_csv(args.csv, args.market_id, args.source, args.output, detect_trends)
        print(f"Detected {len(results['alerts'])} alerts and {len(results['trends'])} trends")
    else:
        results = detector.process_all_markets(detect_trends)
        print(f"Processed {results['stats']['polymarket']['markets']} Polymarket markets")
        print(f"  - Alerts: {results['stats']['polymarket']['alerts']}")
        print(f"  - Trends: {results['stats']['polymarket']['trends']}")
        print(f"Processed {results['stats']['kalshi']['markets']} Kalshi markets")
        print(f"  - Alerts: {results['stats']['kalshi']['alerts']}")
        print(f"  - Trends: {results['stats']['kalshi']['trends']}")
        print(f"Total signals: {results['total_signals']}")
        print(f"Stored signals: {results['stored_signals']}")


if __name__ == '__main__':
    main()