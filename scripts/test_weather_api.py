"""Debug weather Gamma API queries."""
import requests
import json

print("=== Weather tag ===")
r = requests.get(
    "https://gamma-api.polymarket.com/events",
    params={"tag": "Weather", "closed": "false", "limit": 5},
    timeout=10,
)
data = r.json()
print(f"{r.status_code} | {len(data)} events")
for e in data[:3]:
    slug = e.get("slug", "")
    print(f"  slug={slug[:50]} markets={len(e.get('markets', []))}")
    for m in e.get("markets", [])[:2]:
        print(f"    {m.get('question', '')[:70]}")

print()
print("=== Slug temperature ===")
r2 = requests.get(
    "https://gamma-api.polymarket.com/events",
    params={"slug_contains": "temperature", "closed": "false", "limit": 5},
    timeout=10,
)
data2 = r2.json()
print(f"{r2.status_code} | {len(data2)} events")
for e in data2[:3]:
    print(f"  {e.get('slug', '')[:50]}")

print()
print("=== Slug weather ===")
r3 = requests.get(
    "https://gamma-api.polymarket.com/events",
    params={"slug_contains": "weather", "closed": "false", "limit": 5},
    timeout=10,
)
data3 = r3.json()
print(f"{r3.status_code} | {len(data3)} events")
for e in data3[:3]:
    print(f"  {e.get('slug', '')[:50]}")
    for m in e.get("markets", [])[:2]:
        q = m.get("question", "")[:80]
        print(f"    {q}")

print()
print("=== Direct tag test on first 100 events ===")
r4 = requests.get(
    "https://gamma-api.polymarket.com/events",
    params={"closed": "false", "limit": 100},
    timeout=10,
)
data4 = r4.json()
print(f"First 100 events: {len(data4)}")
# Show available tags
all_tags = set()
for e in data4:
    for t in e.get("tags", []):
        if isinstance(t, dict):
            all_tags.add(t.get("label", t.get("slug", str(t))))
        else:
            all_tags.add(str(t))
print(f"Tags found: {sorted(all_tags)[:30]}")
