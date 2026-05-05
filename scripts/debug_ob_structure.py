"""Debug SC orderbook structure on VPS."""
import sys, os, json
sys.path.insert(0, "/home/ubuntu/nachomarket")
os.chdir("/home/ubuntu/nachomarket")
from dotenv import load_dotenv; load_dotenv()
from src.polymarket.client import PolymarketClient
import requests

client = PolymarketClient(paper_mode=True)

# Get a real market with YES low
r = requests.get("https://gamma-api.polymarket.com/events", params={"closed": "false", "limit": 50}, timeout=10)
for event in r.json():
    for mkt in event.get("markets", []):
        prices = mkt.get("outcomePrices", [])
        if isinstance(prices, str):
            try: prices = json.loads(prices)
            except: prices = []
        if len(prices) >= 2:
            yes = float(prices[0])
            if 0.01 <= yes <= 0.05:
                cids = mkt.get("clobTokenIds", [])
                if isinstance(cids, str):
                    try: cids = json.loads(cids)
                    except: cids = []
                if len(cids) >= 2:
                    yes_tid = str(cids[0])
                    no_tid = str(cids[1])
                    q = mkt.get("question", "")[:60]
                    print(f"Market: {q}")
                    print(f"YES tid: {yes_tid[:20]}...")
                    print(f"NO tid: {no_tid[:20]}...")
                    
                    # Fetch real orderbook
                    ob = client.get_orderbooks_batch([yes_tid, no_tid])
                    print(f"Orderbook type: {type(ob)}")
                    if ob:
                        for k, v in list(ob.items())[:1]:
                            print(f"Key: {k[:20]}...")
                            print(f"Value type: {type(v)}")
                            if isinstance(v, dict):
                                print(f"Value keys: {list(v.keys())[:10]}")
                                asks = v.get("asks", v.get("sell", []))
                                bids = v.get("bids", v.get("buy", []))
                                print(f"Asks sample: {str(asks)[:200]}")
                                print(f"Bids sample: {str(bids)[:200]}")
                                if asks:
                                    a0 = asks[0]
                                    print(f"Ask[0] type={type(a0)} value={a0}")

                    # Also check: get best_bid_ask
                    bba = client.get_best_bid_ask(no_tid)
                    print(f"Best bid/ask NO: {bba}")
                    
                    import sys; sys.exit(0)
