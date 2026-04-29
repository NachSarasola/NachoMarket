import subprocess

def ssh(cmd):
    result = subprocess.run(
        ['ssh', 'dublin', cmd],
        capture_output=True, text=True, timeout=30
    )
    return result.stdout + result.stderr

# 1. Check open orders via REST API
print("=== ORDERS ON BOOK ===")
out = ssh("curl -s 'https://clob.polymarket.com/orders?address=0x7B2833091e32343565D6E3Fb99A09e2DF60471a1' | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps(d[:10], indent=2))\" 2>/dev/null")
print(out if out else "No response")

# 2. Check bot logs - recent orders
print("\n=== BOT LOGS (orders) ===")
out = ssh("sudo journalctl -u polymarket-bot -n 50 | grep -i 'orden\\|order\\|placed\\|filled\\|matched' | tail -20")
print(out[:2000])

# 3. Check positions
print("\n=== POSITIONS ===")
out = ssh("sudo journalctl -u polymarket-bot -n 100 | grep -i 'position\\|get_positions' | tail -10")
print(out[:1000])