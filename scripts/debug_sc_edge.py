"""Debug SC edge calculation on VPS with real orderbook."""
import requests, json, math, os, sys
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/nachomarket")
os.chdir("/home/ubuntu/nachomarket")

from dotenv import load_dotenv
load_dotenv()

from src.polymarket.client import PolymarketClient

GAMMA = "https://gamma-api.polymarket.com/events"

# Fetch markets with YES 0.01-0.20
r = requests.get(GAMMA, params={"closed": "false", "limit": 200}, timeout=10)
events = r.json()

candidates = []
for event in events:
    for mkt in event.get("markets", []):
        prices = mkt.get("outcomePrices", [])
        if isinstance(prices, str):
            try: prices = json.loads(prices)
            except: prices = []
        if len(prices) >= 2:
            try:
                yes = float(prices[0])
                no = float(prices[1])
                if 0.01 <= yes <= 0.20 and no >= 0.80:
                    candidates.append((yes, no, mkt, event.get("slug", "")))
            except: pass

print(f"Candidates with YES [0.01,0.20] NO>=0.80: {len(candidates)}")

# Take top 5 by lowest YES
candidates.sort(key=lambda x: x[0])
top = candidates[:5]

# Get real orderbooks
client = PolymarketClient(paper_mode=True)
all_tids = []
for _, _, mkt, _ in top:
    cids = mkt.get("clobTokenIds", [])
    if isinstance(cids, str):
        try: cids = json.loads(cids)
        except: cids = []
    for tid in cids:
        all_tids.append(str(tid))

obs = client.get_orderbooks_batch(all_tids) if all_tids else {}
print(f"Orderbooks fetched: {len(obs)}")

for yes, no, mkt, slug in top:
    cids = mkt.get("clobTokenIds", [])
    if isinstance(cids, str):
        try: cids = json.loads(cids)
        except: cids = []
    if len(cids) < 2:
        continue
    
    yes_tid = str(cids[0])
    no_tid = str(cids[1])
    
    yes_ob = obs.get(yes_tid, {})
    no_ob = obs.get(no_tid, {})
    
    no_asks = no_ob.get("asks", []) if isinstance(no_ob, dict) else []
    yes_bids = yes_ob.get("bids", []) if isinstance(yes_ob, dict) else []
    
    no_ask = float(no_asks[0][0]) if no_asks else no
    yes_bid = float(yes_bids[0][0]) if yes_bids else yes
    
    buy_price = no_ask - 0.01
    
    # Get end_date for decay
    end_date = mkt.get("endDate", "") or mkt.get("endDateIso", "")
    hours_left = 720  # default 30 days
    if end_date:
        try:
            if end_date.endswith("Z"):
                end_date = end_date[:-1] + "+00:00"
            end_dt = datetime.fromisoformat(end_date)
            now = datetime.now(timezone.utc)
            hours_left = (end_dt - now).total_seconds() / 3600.0
        except:
            pass
    
    days_left = max(0.0, hours_left / 24.0)
    uncertainty = 0.10 * (1.0 - 1.0 / (1.0 + days_left / 7.0))
    uncertainty = min(0.15, uncertainty)
    adjusted_yes = yes * (1.0 - uncertainty)
    adjusted_yes = max(0.001, adjusted_yes)
    est_prob = 1.0 - adjusted_yes
    edge = est_prob - buy_price
    
    q = mkt.get("question", "")[:60]
    print(f"\n{q}")
    print(f"  YES_mid={yes:.4f} NO_mid={no:.4f} NO_ask={no_ask:.4f} YES_bid={yes_bid:.4f}")
    print(f"  buy_price={buy_price:.4f} est_prob={est_prob:.4f} edge={edge:.4f} ({edge*100:.2f}%)")
    print(f"  days_left={days_left:.0f} uncertainty={uncertainty:.4f} adj_yes={adjusted_yes:.4f}")
