import os
os.chdir("/home/ubuntu/nachomarket")
from dotenv import load_dotenv
load_dotenv()
from py_clob_client_v2 import ClobClient, ApiCreds, AssetType, BalanceAllowanceParams

pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
key = os.environ.get("POLYMARKET_API_KEY", "")
secret = os.environ.get("POLYMARKET_SECRET", "")
passphrase = os.environ.get("POLYMARKET_PASSPHRASE", "")
proxy = os.environ.get("POLYMARKET_PROXY_ADDRESS", "")

creds = ApiCreds(api_key=key, api_secret=secret, api_passphrase=passphrase)
client = ClobClient("https://clob.polymarket.com", chain_id=137, key=pk, creds=creds, signature_type=2, funder=proxy)

params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
result = client.get_balance_allowance(params)
print("Balance (default):", result)

params2 = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, address=proxy)
result2 = client.get_balance_allowance(params2)
print("Balance (proxy):", result2)
