"""Debug SC orderbook structure on VPS - LIVE mode."""
import sys, os, json
sys.path.insert(0, "/home/ubuntu/nachomarket")
os.chdir("/home/ubuntu/nachomarket")
from dotenv import load_dotenv; load_dotenv()
from src.polymarket.client import PolymarketClient
import requests

# LIVE mode to get real orderbook data
client = PolymarketClient(paper_mode=False, signature_type=2)

# Get a real market with YES low
r = requests.get("https://gamma-api.polymarket.com/events", params={"closed": "false", "limit": 80}, timeout=10)
found = 0
for event in r.json():
    for mkt in event.get("markets", []):
        prices = mkt.get("outcomePrices", [])
        if isinstance(prices, str):
            try: prices = json.loads(prices)
            except: prices = []
        if len(prices) >= 2:
            yes = float(prices[0])
            if 0.01 <= yes <= 0.10 and found < 3:
                cids = mkt.get("clobTokenIds", [])
                if isinstance(cids, str):
                    try: cids = json.loads(cids)
                    except: cids = []
                if len(cids) >= 2:
                    yes_tid = str(cids[0])
                    no_tid = str(cids[1])
                    q = mkt.get("question", "")[:60]
                    print(f"\nMarket: {q}")
                    
                    ob = client.get_orderbooks_batch([yes_tid, no_tid])
                    for tid, data in ob.items():
                        is_no = (tid == no_tid)
                        label = "NO" if is_no else "YES"
                        asks = data.get("asks", [])
                        bids = data.get("bids", [])
                        ask_price = float(asks[0][0]) if asks else 0
                        bid_price = float(bids[0][0]) if bids else 0
                        print(f"  {label} asks={len(asks)} bids={len(bids)} ask={ask_price:.4f} bid={bid_price:.4f}")
                    
                    # Also try get_best_bid_ask directly
                    try:
                        no_bba = client.get_best_bid_ask(no_tid)
                        yes_bba = client.get_best_bid_ask(yes_tid)
                        print(f"  CLOB NO bba: {no_bba}")
                        print(f"  CLOB YES bba: {yes_bba}")
                    except Exception as e:
                        print(f"  CLOB error: {e}")
                    
                    found += 1
