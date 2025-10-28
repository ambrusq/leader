#!/usr/bin/env python3
"""
Simple web server for Render deployment
Provides an HTTP endpoint that triggers data collection
"""

import os
import json
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import logging

# Import the collector
from poly_collector import PolymarketCollector
from poly_price_collector import PolymarketPriceCollector
from kalshi_collector import KalshiCollector


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CollectorHandler(BaseHTTPRequestHandler):
    """HTTP request handler for triggering collection"""
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urlparse(self.path)
        
        # Health check endpoint
        if parsed_path.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                'status': 'healthy',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            self.wfile.write(json.dumps(response).encode())
            return
        
        # Polymarket collection endpoint
        if parsed_path.path == '/collect':
            try:
                logger.info("Polymarket collection triggered via HTTP")
                collector = PolymarketCollector()
                stats = collector.collect_all()
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                response = {
                    'status': 'success',
                    'platform': 'polymarket',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'stats': stats
                }
                self.wfile.write(json.dumps(response).encode())
                
            except Exception as e:
                logger.error(f"Polymarket collection error: {e}", exc_info=True)
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    'status': 'error',
                    'platform': 'polymarket',
                    'error': str(e),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                self.wfile.write(json.dumps(response).encode())
            return
        
        # Polymarket price collection endpoint
        if parsed_path.path == '/collect-prices':
            try:
                logger.info("Polymarket price collection triggered via HTTP")
                collector = PolymarketPriceCollector()
                stats = collector.collect_all_prices()
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                response = {
                    'status': 'success',
                    'platform': 'polymarket',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'stats': stats
                }
                self.wfile.write(json.dumps(response).encode())
                
            except Exception as e:
                logger.error(f"Polymarket price collection error: {e}", exc_info=True)
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    'status': 'error',
                    'platform': 'polymarket',
                    'error': str(e),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                self.wfile.write(json.dumps(response).encode())
            return
        
        # Kalshi price collection endpoint
        if parsed_path.path == '/collect-kalshi':
            try:
                logger.info("Kalshi price collection triggered via HTTP")
                collector = KalshiCollector()
                stats = collector.collect_all_prices()
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                response = {
                    'status': 'success',
                    'platform': 'kalshi',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'stats': stats
                }
                self.wfile.write(json.dumps(response).encode())
                
            except Exception as e:
                logger.error(f"Kalshi price collection error: {e}", exc_info=True)
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    'status': 'error',
                    'platform': 'kalshi',
                    'error': str(e),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                self.wfile.write(json.dumps(response).encode())
            return
        
        # Combined collection endpoint (both platforms)
        if parsed_path.path == '/collect-all':
            try:
                logger.info("Combined collection triggered via HTTP")
                results = {}
                
                # Collect Polymarket prices
                try:
                    poly_collector = PolymarketPriceCollector()
                    results['polymarket'] = poly_collector.collect_all_prices()
                except Exception as e:
                    logger.error(f"Polymarket collection failed: {e}")
                    results['polymarket'] = {'error': str(e)}
                
                # Collect Kalshi prices
                try:
                    kalshi_collector = KalshiCollector()
                    results['kalshi'] = kalshi_collector.collect_all_prices()
                except Exception as e:
                    logger.error(f"Kalshi collection failed: {e}")
                    results['kalshi'] = {'error': str(e)}
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                response = {
                    'status': 'success',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'results': results
                }
                self.wfile.write(json.dumps(response).encode())
                
            except Exception as e:
                logger.error(f"Combined collection error: {e}", exc_info=True)
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                self.wfile.write(json.dumps(response).encode())
            return
        
        # Root endpoint
        if parsed_path.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                'service': 'Prediction Markets Data Collector',
                'platforms': ['Polymarket', 'Kalshi'],
                'endpoints': {
                    '/health': 'Health check',
                    '/collect': 'Trigger Polymarket data collection',
                    '/collect-prices': 'Trigger Polymarket price collection',
                    '/collect-kalshi': 'Trigger Kalshi price collection',
                    '/collect-all': 'Trigger collection for all platforms',
                    '/kalshi/add-market': 'POST: Add a Kalshi market to tracking'
                },
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            self.wfile.write(json.dumps(response).encode())
            return
        
        # 404 for other paths
        self.send_response(404)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {'error': 'Not found'}
        self.wfile.write(json.dumps(response).encode())
    
    def log_message(self, format, *args):
        """Override to use logger"""
        logger.info("%s - %s" % (self.client_address[0], format % args))


def run_server(port=8000):
    """Run the HTTP server"""
    server_address = ('', port)
    httpd = HTTPServer(server_address, CollectorHandler)
    logger.info(f'Starting server on port {port}')
    httpd.serve_forever()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    run_server(port)