"""Verifica estado de ordenes abiertas: precios, shadow gap, fill rate, rewards scoring."""
import sys, os
sys.path.insert(0, os.path.expanduser("~/nachomarket"))

from dotenv import load_dotenv
load_dotenv(os.path.expanduser("~/nachomarket/.env"))

import yaml
with open(os.path.expanduser("~/nachomarket/config/settings.yaml")) as f:
    cfg = yaml.safe_load(f)

from src.polymarket.client import PolymarketClient

client = PolymarketClient(
    signature_type=cfg.get("signature_type", 2),
    paper_mode=False,
)

def parse_book(book):
    """Normaliza respuesta de orderbook (dict o objeto)."""
    if isinstance(book, dict):
        bids_raw = book.get("bids", [])
        asks_raw = book.get("asks", [])
    else:
        bids_raw = getattr(book, "bids", [])
        asks_raw = getattr(book, "asks", [])

    def price_size(entry):
        if isinstance(entry, dict):
            return float(entry.get("price", 0)), float(entry.get("size", 0))
        return float(getattr(entry, "price", 0)), float(getattr(entry, "size", 0))

    bids = sorted([price_size(b) for b in bids_raw], reverse=True)
    asks = sorted([price_size(a) for a in asks_raw])
    return bids, asks

positions = client.get_positions()
print(f"Ordenes abiertas: {len(positions)}")

for p in positions:
    oid      = p.get("id", p.get("order_id", "?"))
    tid      = p.get("asset_id", p.get("token_id", "?"))
    side     = p.get("side", "?")
    price    = float(p.get("price", 0))
    size     = float(p.get("original_size", p.get("size", 0)))
    matched  = float(p.get("size_matched", 0))
    remaining = size - matched

    print(f"\n  {side} {size:.0f}sh @ {price:.3f}  filled={matched:.1f}  remaining={remaining:.1f}")
    print(f"  token={tid[:22]}...")

    try:
        raw_book = client._client.get_order_book(tid)
        bids, asks = parse_book(raw_book)
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 1.0
        mid  = round((best_bid + best_ask) / 2, 4)
        gap  = round(best_bid - price, 3)
        if gap >= 0.02:
            safety = "SEGURO (>2c del BBO)"
        elif gap >= 0.01:
            safety = "OK (1-2c del BBO)"
        elif gap > 0:
            safety = "CHICO (<1c del BBO)"
        else:
            safety = "RIESGO (encima o igual al BBO)"
        print(f"  BBO:  bid={best_bid:.3f}  ask={best_ask:.3f}  mid={mid:.4f}")
        print(f"  Orden@{price:.3f}  best_bid@{best_bid:.3f}  gap={gap:+.3f}  -> {safety}")
        top3 = [f"{px:.3f}x{sz:.0f}" for px, sz in bids[:3]]
        print(f"  Top 3 bids: {' | '.join(top3)}")
    except Exception as e:
        print(f"  Orderbook error: {e}")

    try:
        scoring = client.is_order_scoring(oid)
        print(f"  Rewards scoring: {scoring}")
    except Exception as e:
        print(f"  Scoring error: {e}")

# Balance via CLOB
try:
    import requests, json
    from py_clob_client.headers.headers import create_l1_or_l2_headers
    host = "https://clob.polymarket.com"
    bal_resp = requests.get(f"{host}/balance", headers=client._client.get_auth_headers() if hasattr(client._client, 'get_auth_headers') else {}, timeout=5)
    print(f"\nBalance API: {bal_resp.status_code} {bal_resp.text[:80]}")
except Exception as e:
    print(f"\nBalance check error: {e}")
