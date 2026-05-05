"""Debug SC edge and weather market discovery."""
import requests

GAMMA = "https://gamma-api.polymarket.com/events"

print("=== Checking real market prices (first 100 events) ===")
r = requests.get(GAMMA, params={"closed": "false", "limit": 100}, timeout=10)
events = r.json()

temp_keywords = ["temperature", "temp", "°f", "degrees", "high", "low"]
temp_found = 0
yes_prices = []
no_asks = []

for event in events:
    for mkt in event.get("markets", []):
        question = (mkt.get("question", "") or mkt.get("groupItemTitle", "")).lower()
        
        # Check temperature keywords
        for kw in temp_keywords:
            if kw in question:
                temp_found += 1
                print(f"  TEMP: {question[:80]}")
                break
        
        # Check prices for SC analysis
        prices = mkt.get("outcomePrices", [])
        if isinstance(prices, str):
            import json
            try:
                prices = json.loads(prices)
            except:
                prices = []
        if len(prices) >= 2:
            try:
                yes = float(prices[0])
                no = float(prices[1])
                if 0.01 <= yes <= 0.20 and no >= 0.80:
                    yes_prices.append((yes, no, question[:60]))
            except (ValueError, TypeError):
                pass

print(f"\nTemperature keyword matches in 100 events: {temp_found}")
print(f"\nMarkets with YES in [0.01,0.20] and NO >= 0.80: {len(yes_prices)}")
for yes, no, q in yes_prices[:10]:
    print(f"  YES={yes:.3f} NO={no:.3f} | {q}")

print("\n=== Checking orderbook for a sample market ===")
# Find first market with YES < 0.10
target = None
for event in events:
    for mkt in event.get("markets", []):
        prices = mkt.get("outcomePrices", [])
        if isinstance(prices, str):
            import json
            try: prices = json.loads(prices)
            except: prices = []
        if len(prices) >= 2:
            try:
                if float(prices[0]) < 0.10:
                    target = mkt
                    break
            except: pass
    if target: break

if target:
    clob_ids = target.get("clobTokenIds", [])
    if isinstance(clob_ids, str):
        import json
        try: clob_ids = json.loads(clob_ids)
        except: clob_ids = []
    if len(clob_ids) >= 2:
        yes_tid = clob_ids[0]
        no_tid = clob_ids[1]
        print(f"Market: {target.get('question','')[:60]}")
        print(f"YES={target.get('outcomePrices',[])}")
        # Fetch orderbook
        from src.polymarket.client import PolymarketClient
        import os
        from dotenv import load_dotenv
        load_dotenv()
        client = PolymarketClient(paper_mode=True)
        try:
            ob = client.get_orderbooks_batch([str(yes_tid), str(no_tid)])
            print(f"Orderbook: {ob}")
        except Exception as e:
            print(f"Orderbook error: {e}")
