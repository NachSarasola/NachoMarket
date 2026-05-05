"""Debug earnings format."""
import os, sys, json
os.chdir("/home/ubuntu/nachomarket")
sys.path.insert(0, "/home/ubuntu/nachomarket")
from dotenv import load_dotenv; load_dotenv()
from src.polymarket.client import PolymarketClient

c = PolymarketClient(paper_mode=False, signature_type=2)
data = c.get_user_earnings_markets()

# Show raw structure of first item with earnings > 0
found = False
for d in data:
    earnings_list = d.get("earnings", [])
    if isinstance(earnings_list, list):
        for e in earnings_list:
            if isinstance(e, dict) and float(e.get("earnings", 0)) > 0:
                print(json.dumps(e, indent=2))
                print(f"\ncondition_id: {d.get('condition_id','')}")
                print(f"earning_percentage: {d.get('earning_percentage','')}")
                print(f"question: {d.get('question','')[:60]}")
                found = True
                break
    if found:
        break

if not found:
    print("No earnings > 0 found. Showing raw structure of first item:")
    if data:
        first = data[0]
        print(json.dumps({k: str(v)[:200] for k, v in first.items()}, indent=2))

# Also check: are there markets where user has positions?
positions = c.get_positions()
print(f"\nOpen orders: {len(positions)}")
for p in positions[:3]:
    print(f"  cid={p.get('condition_id','')[:14]} side={p.get('side','')} price={p.get('price','')}")
