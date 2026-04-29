import asyncio
import json
from websockets.asyncio.client import connect

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
TOKEN_YES = "13915632006860532760341456059417205520635074812665933636242167054302381929047"
TOKEN_NO = "13290642992840049713498455047821992687320520126780477212439849996225688703232"

async def test_official_format():
    print("=== Official format: type=market, custom_feature_enabled=true ===")
    async with connect(WS_URL, ping_interval=30, ping_timeout=10, open_timeout=15) as ws:
        print("Connected")
        await ws.send(json.dumps({
            "type": "market",
            "assets_ids": [TOKEN_YES, TOKEN_NO],
            "custom_feature_enabled": True
        }))
        print("Sent subscription")
        
        msg_count = 0
        for i in range(120):
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                msg_count += 1
                data = json.loads(raw)
                if isinstance(data, list):
                    print(f"  #{msg_count} LIST len={len(data)} first={json.dumps(data[0])[:400] if data else 'empty'}")
                elif isinstance(data, dict):
                    etype = data.get('event_type', data.get('type', 'unknown'))
                    print(f"  #{msg_count} DICT type={etype}")
                    if etype == 'book':
                        print(f"      asset_id={data.get('asset_id','')[:16]}... bids={len(data.get('bids',[]))} asks={len(data.get('asks',[]))}")
                    elif etype == 'price_change':
                        print(f"      changes={len(data.get('price_changes',[]))}")
                else:
                    print(f"  #{msg_count} {type(data).__name__}: {str(data)[:200]}")
            except asyncio.TimeoutError:
                if i % 20 == 0:
                    print(f"  ... {i*0.5:.0f}s, msgs={msg_count}")
                continue
            except Exception as e:
                print(f"  Error: {e}")
                break
        print(f"  Done. Msgs={msg_count}")

asyncio.run(test_official_format())
