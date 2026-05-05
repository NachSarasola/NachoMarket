import os, json
os.chdir("/home/ubuntu/nachomarket")
from dotenv import load_dotenv; load_dotenv()
from src.kalshi.client import KalshiClient

c = KalshiClient()

series = c.get_series("Climate and Weather", 10)
print(f"Series found: {len(series)}")
for s in series[:10]:
    print(f"  ticker={s.get('ticker','')} title={s.get('title','')[:50]}")

print()
mkts = c.get_markets(None, "open", 5)
print(f"All open markets: {len(mkts)}")
for m in mkts[:5]:
    print(f"  {m.get('ticker','')}")

print()
# Try specific known series
for s in ["KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHLAX"]:
    mkts = c.get_markets(s, "open", 3)
    print(f"{s}: {len(mkts)} open mkts")
    for m in mkts[:2]:
        print(f"  {m.get('ticker','')} yes_bid={m.get('yes_bid')} yes_ask={m.get('yes_ask')}")
