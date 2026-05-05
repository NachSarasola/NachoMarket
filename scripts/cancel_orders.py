"""Cancel bad orders before restart."""
import os, json, sys
os.chdir("/home/ubuntu/nachomarket")
sys.path.insert(0, "/home/ubuntu/nachomarket")
from dotenv import load_dotenv; load_dotenv()
from src.polymarket.client import PolymarketClient

c = PolymarketClient(paper_mode=False, signature_type=2)
orders = c.get_positions()
print(f"Open orders: {len(orders)}")

# Cancel delayed weather orders
delayed = [
    "0x214f539b917577277da71063ea3a1b5586e2ea82f3f9eb401c43568f1d208324",
    "0x33bc6481190e50ba7ac82718b3b1f7cba09da79f4d301e1f6d49f241e8a50736",
]
for oid in delayed:
    try:
        c.cancel_order(oid)
        print(f"Cancelled delayed: {oid[:14]}")
    except Exception as e:
        print(f"Cancel failed {oid[:14]}: {e}")

# Cancel high-price SC orders (> 0.93)
for o in orders:
    price = float(o.get("price", 0))
    oid = o.get("id") or o.get("order_id", "")
    side = o.get("side", "")
    if price > 0.93 and side == "BUY":
        try:
            c.cancel_order(oid)
            print(f"Cancelled high-price SC: {oid[:14]} @ ${price:.4f}")
        except Exception as e:
            print(f"Cancel failed {oid[:14]}: {e}")

print("Done")
