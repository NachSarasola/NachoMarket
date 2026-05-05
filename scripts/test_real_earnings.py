"""Test real earnings endpoint."""
import os, sys, json
os.chdir("/home/ubuntu/nachomarket")
sys.path.insert(0, "/home/ubuntu/nachomarket")
from dotenv import load_dotenv; load_dotenv()
from src.polymarket.client import PolymarketClient

c = PolymarketClient(paper_mode=False, signature_type=2)
data = c.get_user_earnings_markets()

print(f"Mercados con earnings: {len(data)}")
total = 0.0
for d in data[:10]:
    cid = d.get("condition_id", "")[:14]
    earnings_list = d.get("earnings", [])
    earnings = sum(float(e.get("earnings", 0)) for e in earnings_list) if earnings_list else 0
    pct = d.get("earning_percentage", 0)
    configs = d.get("rewards_config", [])
    rate = sum(float(r.get("rate_per_day", 0)) for r in configs) if configs else 0
    total += earnings
    q = d.get("question", "")[:50]
    print(f"  {cid} | ${earnings:.4f} | {pct:.0f}% share | ${rate:.0f}/d | {q}")

print(f"\nTotal real earnings hoy: ${total:.4f}")
# Compare with get_daily_real_rewards
real_total = c.get_daily_real_rewards()
print(f"get_daily_real_rewards: ${real_total:.4f}")
