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
    "will-inflation-reach-more-than-5-in-2025",
    "will-inflation-reach-more-than-6-in-2025",
    "will-inflation-reach-more-than-8-in-2025",
    "will-inflation-reach-more-than-4-in-2025",
    "will-inflation-reach-more-than-3-in-2025",
    "will-inflation-reach-more-than-10-in-2025",

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