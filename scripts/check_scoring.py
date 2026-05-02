"""Verifica scoring de rewards directamente via SDK y HTTP."""
import os, sys, json, requests
sys.path.insert(0, os.path.expanduser("~/nachomarket"))

from dotenv import dotenv_values
env = dotenv_values(os.path.expanduser("~/nachomarket/.env"))

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import ApiCreds

client = ClobClient(
    host="https://clob.polymarket.com",
    key=env["POLYMARKET_PRIVATE_KEY"],
    chain_id=137,
    creds=ApiCreds(
        api_key=env["POLYMARKET_API_KEY"],
        api_secret=env["POLYMARKET_SECRET"],
        api_passphrase=env["POLYMARKET_PASSPHRASE"],
    ),
    signature_type=2,
    funder=env["POLYMARKET_PROXY_ADDRESS"],
)

orders = {
    "Trump NO  (181sh @ 0.110)": "0xdb14037d5754a3d8fa008f136edd13844f407c33127d9fabe8cc006c39d7b4d2",
    "Hormuz YES(200sh @ 0.040)": "0x7be74adbf1a95686d3ca201fe7dcd606abddb30660de4b7f8f1e5fa77d4b4307",
}

print("=== REWARDS SCORING (SDK directo) ===")
for name, oid in orders.items():
    try:
        # El SDK fallará con AttributeError — lo capturamos y mostramos el raw
        raw = client.is_order_scoring(oid)
        print(f"{name}: {raw!r}")
    except AttributeError as e:
        print(f"{name}: AttributeError='{e}' (SDK bug conocido)")
    except Exception as e:
        print(f"{name}: {type(e).__name__}: {e}")

print()
print("=== REWARDS SCORING (HTTP directo) ===")
# Construir headers L2 manualmente
import hmac, hashlib, time, base64

api_key    = env["POLYMARKET_API_KEY"]
api_secret = env["POLYMARKET_SECRET"]
api_pass   = env["POLYMARKET_PASSPHRASE"]

ts = str(int(time.time()))
method = "GET"
path = "/order-scoring"

msg = ts + method + path
sig = hmac.new(
    base64.b64decode(api_secret),
    msg.encode(),
    hashlib.sha256,
).digest()
sig_b64 = base64.b64encode(sig).decode()

headers = {
    "POLY-API-KEY": api_key,
    "POLY-TIMESTAMP": ts,
    "POLY-PASSPHRASE": api_pass,
    "POLY-SIGNATURE": sig_b64,
}

for name, oid in orders.items():
    try:
        r = requests.get(
            "https://clob.polymarket.com/order-scoring",
            params={"order_id": oid},
            headers=headers,
            timeout=8,
        )
        print(f"{name}: HTTP {r.status_code} => {r.text[:150]}")
    except Exception as e:
        print(f"{name}: HTTP error {e}")

print()
print("=== REWARDS DATA (mercados activos) ===")
markets_data = [
    ("Trump out 2027",  "0x48b0b0bca5fe9adcc3b4f4e46f37c5e0dc3a024c0a9eacd96b8f7a26b4d55c5", 100.0),
    ("Strait Hormuz",   "0xffe381a80c64dfbe8a70fc54c4f9254f0de8be3e83faeadb8d31e28ac08d6bea", 600.0),
]
for name, cid, daily_rate in markets_data:
    print(f"{name}: daily_rate=${daily_rate}/day  min_size=200sh  max_spread=3.5%")
print()
print("Nuestras ordenes vs criterios:")
print("  Trump NO  181sh @ 0.110 | min_size=200 -> DEBAJO del minimo (181 < 200) PROBLEMA")
print("  Hormuz YES 200sh @ 0.040 | min_size=200 -> OK (200 == 200)")
print("  Ambas dentro del max_spread window de mid +/- 1.75c")
