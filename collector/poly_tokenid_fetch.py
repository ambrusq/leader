import requests
import json
import csv

def get_market_info_from_slug(slug):
    """
    Get market info including token IDs using the slug from the URL.
    Returns a dictionary with the market info or None if not found.
    """
    # Method 1: Try direct slug endpoint (this should work for all!)
    gamma_slug_url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    response = requests.get(gamma_slug_url)
    
    if response.status_code == 200:
        market = response.json()
        token_ids_str = market.get('clobTokenIds', '[]')
        token_ids = json.loads(token_ids_str)
        
        return {
            'slug': slug,
            'tokenid1': token_ids[0] if len(token_ids) > 0 else '',
            'tokenid2': token_ids[1] if len(token_ids) > 1 else '',
            'conditionId': market.get('conditionId', '')
        }
    
    # Method 2: Try the events endpoint as fallback
    gamma_events_url = f"https://gamma-api.polymarket.com/events?slug={slug}"
    response = requests.get(gamma_events_url)
    
    if response.status_code == 200:
        data = response.json()
        if data:
            event = data[0] if isinstance(data, list) else data
            
            if 'markets' in event and len(event['markets']) > 0:
                market = event['markets'][0]
                
                token_ids_str = market.get('clobTokenIds', '[]')
                token_ids = json.loads(token_ids_str)
                
                return {
                    'slug': slug,
                    'tokenid1': token_ids[0] if len(token_ids) > 0 else '',
                    'tokenid2': token_ids[1] if len(token_ids) > 1 else '',
                    'conditionId': market.get('conditionId', '')
                }
    
    # Not found
    return {
        'slug': slug,
        'tokenid1': 'NOT_FOUND',
        'tokenid2': 'NOT_FOUND',
        'conditionId': 'NOT_FOUND'
    }

# Your slugs list
slugs = [
    "fed-decreases-interest-rates-by-50-bps-after-january-2026-meeting",
    "fed-decreases-interest-rates-by-25-bps-after-january-2026-meeting",
    "no-change-in-fed-interest-rates-after-january-2026-meeting",
    "fed-increases-interest-rates-by-25-bps-after-january-2026-meeting",
    "fed-decreases-interest-rates-by-50-bps-after-january-2026-meeting",
    "fed-decreases-interest-rates-by-25-bps-after-january-2026-meeting",
    "will-the-ecb-announce-a-50-bps-decrease-at-the-october-meeting",
    "will-the-ecb-announce-a-25-bps-decrease-at-the-october-meeting",
    "will-the-ecb-announce-no-change-at-the-october-meeting",
    "will-the-ecb-announce-an-increase-at-the-october-meeting",
    "will-gold-close-under-2500-at-the-end-of-2025",
    "will-gold-close-at-2500-2600-at-the-end-of-2025",
    "will-gold-close-at-2600-2700-at-the-end-of-2025",
    "will-gold-close-at-2800-2900-at-the-end-of-2025",
    "will-gold-close-at-2700-2800-at-the-end-of-2025",
    "will-gold-close-at-2900-3000-at-the-end-of-2025",
    "will-gold-close-at-3000-3100-at-the-end-of-2025",
    "will-gold-close-at-3100-3200-at-the-end-of-2025",
    "will-gold-close-at-3200-or-more-at-the-end-of-2025",
    "will-no-fed-rate-cuts-happen-in-2025",
    "will-7-fed-rate-cuts-happen-in-2025",
    "will-1-fed-rate-cut-happen-in-2025",
    "will-2-fed-rate-cuts-happen-in-2025",
    "will-6-fed-rate-cuts-happen-in-2025",
    "will-3-fed-rate-cuts-happen-in-2025",
    "will-4-fed-rate-cuts-happen-in-2025",
    "will-5-fed-rate-cuts-happen-in-2025",
    "will-8plus-fed-rate-cuts-happen-in-2025",

    # add more
]

# Process all slugs and collect results
results = []
for i, slug in enumerate(slugs, 1):
    print(f"Processing {i}/{len(slugs)}: {slug}")
    market_info = get_market_info_from_slug(slug)
    results.append(market_info)
    if market_info['tokenid1'] == 'NOT_FOUND':
        print(f"  ⚠️  NOT FOUND: {slug}")
    else:
        print(f"  ✓ Found")

# Write to CSV
with open('polymarket_tokens.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['slug', 'tokenid1', 'tokenid2', 'conditionId'])
    writer.writeheader()
    writer.writerows(results)

print(f"\n✓ Done! Results saved to polymarket_tokens.csv")
print(f"Processed {len(results)} markets")
print(f"Found: {sum(1 for r in results if r['tokenid1'] != 'NOT_FOUND')}/{len(results)}")