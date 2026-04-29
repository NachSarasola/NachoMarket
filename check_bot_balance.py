import os
os.chdir("/home/ubuntu/nachomarket")
from dotenv import load_dotenv
load_dotenv()
from src.polymarket.client import PolymarketClient

client = PolymarketClient(paper_mode=False, signature_type=2)
balance = client.get_balance()
print(f"Balance reported by bot: {balance}")

# Also check positions
positions = client.get_positions()
print(f"Open positions: {len(positions)}")
for p in positions:
    print(f"  {p}")
