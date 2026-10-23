"""
Verification script to test connection and authentication against the live endpoint.
Reads API_KEY and SECRET_KEY from .env and reports the exact live provider response.
"""

import asyncio
import json
import os
import sys

# Ensure repository root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import websockets
from app.config import settings
from app.ingestion.authentication import FeedAuthenticator

async def verify():
    print("\n" + "=" * 70)
    print("   LIVEFXHUB WEBSOCKET ENDPOINT & AUTHENTICATION VERIFIER")
    print("=" * 70)
    print(f"Target URL : {settings.external_ws_url}")
    print(f"API_KEY    : {settings.api_key[:6]}... (Length: {len(settings.api_key)})")
    print(f"SECRET_KEY : {'*' * 8}... (Length: {len(settings.api_secret)})")
    print("-" * 70)

    authenticator = FeedAuthenticator(api_key=settings.api_key, api_secret=settings.api_secret)

    if not authenticator.has_credentials:
        print("[ERROR] No credentials found in .env! Please set API_KEY and SECRET_KEY in .env.")
        return

    print("Connecting to live WebSocket endpoint...")
    try:
        async with websockets.connect(
            settings.external_ws_url,
            open_timeout=10.0,
            close_timeout=5.0,
        ) as ws:
            print("[SUCCESS] Connected to live endpoint. Transmitting authentication frame:")
            auth_payload = authenticator.create_auth_payload()
            print(f"Auth payload action: 'auth'")
            await ws.send(auth_payload)

            print("Waiting for provider response...")
            resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
            print(f"Raw Provider Response: {resp}")

            is_ok, reason = authenticator.evaluate_auth_response(resp)
            print(f"Validation Result    : {'[AUTHENTICATED]' if is_ok else '[FAILED]'}")
            print(f"Details              : {reason}")

            if is_ok:
                sub_msg = {"action": "subscribeMarketPrice", "symbols": settings.subscribe_symbols}
                print(f"Sending subscription for: {settings.subscribe_symbols}")
                await ws.send(json.dumps(sub_msg))
                print("Listening for incoming live ticks...")
                for i in range(3):
                    tick = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    print(f"Live Tick #{i+1}: {tick}")
            else:
                print("\n[NOTE] Provider rejected credentials with 'invalid credentials'.")
                print("If this API key was recently issued, confirm with your team that the key is activated on the livefxhub server.")

    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n[CLOSED] Connection closed by server: {e}")
    except Exception as e:
        print(f"\n[ERROR] Connection error: {type(e).__name__}: {e}")

    print("=" * 70 + "\n")

if __name__ == "__main__":
    asyncio.run(verify())
