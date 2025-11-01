# Process CSV file (Kalshi format)
python simple_signal_detector.py --csv Kalshi_KXAAAGASM-25OCT31-3.30.csv --market-id KXAAAGASM-25OCT31-3.30 --source kalshi --threshold 0.05

# Process CSV file (Polymarket format)
python simple_signal_detector.py --csv Polymarlet_xi-jinping-out-before-2027.csv --market-id xi-jinping-2027 --source polymarket --threshold 0.05

# CLI usage
python signal_detector.py --threshold 0.05 --trend-threshold 0.20 --trend-window 12 --trend-stability 4

# HTTP endpoint
curl "http://localhost:8000/detect-signals?threshold=0.05&trend_threshold=0.20&trend_window=12&trend_stability=4"

# Disable trends (alerts only)
curl "http://localhost:8000/detect-signals?no_trends=true"
