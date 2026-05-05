import json, sys
trades = []
with open("/home/ubuntu/nachomarket/data/trades.jsonl") as f:
    for line in f:
        if line.strip():
            trades.append(json.loads(line))

weather_t = [t for t in trades if t.get("strategy_name") == "weather"]
sc_t = [t for t in trades if t.get("strategy_name") == "safe_compounder"]

print(f"Weather trades: {len(weather_t)}")
for t in weather_t[-10:]:
    print(f"  {t.get('timestamp','')[:19]} {t.get('side')} ${t.get('size',0)} @ {t.get('price',0):.4f} status={t.get('status')}")

print(f"\nSafeCompounder trades: {len(sc_t)}")
for t in sc_t[-15:]:
    print(f"  {t.get('timestamp','')[:19]} {t.get('side')} ${t.get('size',0)} @ {t.get('price',0):.4f} status={t.get('status')} oid={str(t.get('order_id',''))[:12]}")

# Check for fills (status changes)
print("\n=== FILL DETECTION ===")
statuses = {}
for t in trades:
    s = t.get("status", "unknown")
    sn = t.get("strategy_name", "raw")
    key = f"{sn}:{s}"
    statuses[key] = statuses.get(key, 0) + 1
for k, v in sorted(statuses.items()):
    print(f"  {k}: {v}")
