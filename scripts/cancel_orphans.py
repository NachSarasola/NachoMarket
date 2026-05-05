"""Cancel orphan SC + Weather orders, keep only RF orders."""
import os, sys, json
os.chdir("/home/ubuntu/nachomarket")
sys.path.insert(0, "/home/ubuntu/nachomarket")
from dotenv import load_dotenv; load_dotenv()
from src.polymarket.client import PolymarketClient

c = PolymarketClient(paper_mode=False, signature_type=2)
orders = c.get_positions()
print(f"Total open orders: {len(orders)}")

cancelled = 0
kept = 0
for o in orders:
    oid = o.get("id") or o.get("order_id", "")
    token_id = o.get("token_id", "") or o.get("asset_id", "")
    side = o.get("side", "")
    price = float(o.get("price", 0))
    size = float(o.get("original_size", o.get("size", 0)))
    status = o.get("status", "")
    
    # RF orders: capital $30+ range (not $5 or $8)
    # SC orders are ~$5, Weather are ~$2.5-8
    if size > 40 and price < 0.50:
        kept += 1
        print(f"  KEEP RF: {oid[:16]}... {side} {size:.0f} @ ${price:.4f}")
    elif oid:
        try:
            c.cancel_order(oid)
            cancelled += 1
            print(f"  CANCEL: {oid[:16]}... {side} {size:.0f} @ ${price:.4f}")
        except Exception as e:
            print(f"  FAIL: {oid[:16]}... {e}")

print(f"\nKept (RF): {kept}, Cancelled (SC/Weather): {cancelled}")
