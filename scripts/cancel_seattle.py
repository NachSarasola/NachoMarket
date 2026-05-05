"""Cancel Seattle weather order."""
import os, sys
os.chdir("/home/ubuntu/nachomarket")
sys.path.insert(0, "/home/ubuntu/nachomarket")
from dotenv import load_dotenv; load_dotenv()
from src.polymarket.client import PolymarketClient

c = PolymarketClient(paper_mode=False, signature_type=2)
orders = c.get_positions()
print(f"Remaining: {len(orders)}")
for o in orders:
    oid = o.get("id") or o.get("order_id", "")
    price = float(o.get("price", 0))
    size = float(o.get("original_size", o.get("size", 0)))
    if size == 150 and abs(price - 0.054) < 0.01:
        c.cancel_order(oid)
        print(f"Cancelled Seattle weather: {oid[:16]}...")
    else:
        token = o.get("token_id", "")[:12]
        print(f"  OK: {oid[:16]}... {size:.0f} @ ${price:.4f} ({token})")
print("Done")
