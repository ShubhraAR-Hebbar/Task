"""
Probe live external endpoint wss://v3-prod.livefxhub.com/ws/algo
"""
import asyncio
import json
import ssl
import websockets

async def probe():
    url = "wss://v3-prod.livefxhub.com/ws/algo"
    print(f"Connecting to {url}...")
    try:
        async with websockets.connect(url, open_timeout=8.0) as ws:
            print("[SUCCESS] Successfully connected to livefxhub WebSocket endpoint!")
            auth_msg = {"action": "auth", "api_key": "PROBE_KEY", "secret": "PROBE_SECRET"}
            print(f"Sending probe auth: {auth_msg}")
            await ws.send(json.dumps(auth_msg))
            resp = await asyncio.wait_for(ws.recv(), timeout=8.0)
            print(f"Received actual response from live server: {resp}")
    except Exception as e:
        print(f"[LIVE ENDPOINT RESULT] {type(e).__name__}: {e}")

if __name__ == "__main__":
    asyncio.run(probe())
