#!/usr/bin/env python3
"""
Event Management Utility
Helps manage events and their markets
"""
from dotenv import load_dotenv
load_dotenv()

import sys
from collector import PolymarketCollector
import json

def list_events(collector):
    """List all tracked events"""
    result = collector.supabase.table('polymarket_events')\
        .select('event_slug, title, event_type, closed, active')\
        .execute()
    
    print("\n📊 Tracked Events:")
    print("-" * 80)
    for event in result.data:
        status = "🔴 CLOSED" if event['closed'] else "🟢 ACTIVE"
        type_emoji = "🔢" if event['event_type'] == 'multi_outcome' else "⚪"
        print(f"{type_emoji} {status} | {event['event_slug']}")
        print(f"   {event['title']}")
        print()

def show_event_markets(collector, event_slug):
    """Show all markets for an event"""
    # Get event
    event_result = collector.supabase.table('polymarket_events')\
        .select('*')\
        .eq('event_slug', event_slug)\
        .execute()
    
    if not event_result.data:
        print(f"❌ Event not found: {event_slug}")
        return
    
    event = event_result.data[0]
    event_id = event['id']
    
    print(f"\n📋 Event: {event['title']}")
    print(f"Slug: {event_slug}")
    print(f"Type: {event.get('event_type', 'unknown')}")
    print("-" * 80)
    
    # Get markets
    markets_result = collector.supabase.table('polymarket_tracked_markets')\
        .select('market_slug, market_title, outcome_label, active')\
        .eq('event_id', event_id)\
        .execute()
    
    if not markets_result.data:
        print("No markets found for this event")
        return
    
    print(f"\nMarkets ({len(markets_result.data)}):")
    for i, market in enumerate(markets_result.data, 1):
        status = "✅" if market['active'] else "❌"
        print(f"{i}. {status} {market['market_slug']}")
        if market.get('outcome_label'):
            print(f"   Outcome: {market['outcome_label']}")
        print()

def add_event(collector, event_slug):
    """Add a new event and sync its markets"""
    print(f"\n🔄 Syncing event: {event_slug}")
    synced_slugs = collector.sync_event_markets(event_slug)
    
    if synced_slugs:
        print(f"\n✅ Successfully synced {len(synced_slugs)} markets:")
        for slug in synced_slugs:
            print(f'"{slug}",')
    else:
        print(f"\n❌ Failed to sync event: {event_slug}")

def remove_event(collector, event_slug):
    """Deactivate an event and its markets"""
    # Get event
    event_result = collector.supabase.table('polymarket_events')\
        .select('id')\
        .eq('event_slug', event_slug)\
        .execute()
    
    if not event_result.data:
        print(f"❌ Event not found: {event_slug}")
        return
    
    event_id = event_result.data[0]['id']
    
    # Deactivate event
    collector.supabase.table('polymarket_events')\
        .update({'active': False})\
        .eq('id', event_id)\
        .execute()
    
    # Deactivate all markets
    collector.supabase.table('polymarket_tracked_markets')\
        .update({'active': False})\
        .eq('event_id', event_id)\
        .execute()
    
    print(f"✅ Deactivated event and its markets: {event_slug}")

def main():
    collector = PolymarketCollector()
    
    if len(sys.argv) < 2:
        print("""
Usage:
  python manage_events.py list                      - List all events
  python manage_events.py show <event-slug>         - Show event details
  python manage_events.py add <event-slug>          - Add/sync event
  python manage_events.py remove <event-slug>       - Deactivate event
        
Examples:
  python manage_events.py add fed-decision-in-january
  python manage_events.py show fed-decision-in-january
  python manage_events.py list
        """)
        return
    
    command = sys.argv[1]
    
    if command == 'list':
        list_events(collector)
    
    elif command == 'show' and len(sys.argv) > 2:
        show_event_markets(collector, sys.argv[2])
    
    elif command == 'add' and len(sys.argv) > 2:
        add_event(collector, sys.argv[2])
    
    elif command == 'remove' and len(sys.argv) > 2:
        remove_event(collector, sys.argv[2])
    
    else:
        print("❌ Invalid command or missing arguments")

if __name__ == '__main__':
    main()