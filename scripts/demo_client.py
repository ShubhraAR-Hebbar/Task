"""
Demonstration Client for Outbound WebSocket Server.
Subscribes to live market symbols, displays live updates, and measures latency & payload sizes.
"""

import asyncio
import json
import os
import sys
import time

# Ensure repository root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import websockets


async def run_client(uri: str = "ws://localhost:8765", symbols: list = None, max_messages: int = 50):
    if symbols is None:
        symbols = ["EURUSD", "BTCUSD", "ETHUSD"]

    print(f"\n[CLIENT] Connecting to Pricing Engine at {uri}...")
    async with websockets.connect(uri) as ws:
        print(f"[CLIENT] Connected. Subscribing to symbols: {symbols}")

        # Send subscription request
        sub_req = json.dumps({"action": "subscribe", "symbols": symbols})
        await ws.send(sub_req)

        count = 0
        print("\n" + "=" * 90)
        print(f"{'TIME':<10} | {'SYMBOL':<8} | {'BUY':<10} | {'SELL':<10} | {'24H HIGH':<10} | {'24H LOW':<10} | {'CHANGE %':<10} | {'BYTES':<6}")
        print("=" * 90)

        while count < max_messages:
            msg_raw = await ws.recv()
            byte_len = len(msg_raw.encode("utf-8"))
            parsed = json.loads(msg_raw)
            msg_type = parsed.get("type")

            if msg_type == "subscribed":
                print(f"[ACK] Subscribed successfully to: {parsed.get('symbols')}")
                continue

            if msg_type == "snapshot":
                print(f"[SNAPSHOT] Received initial snapshot with {parsed.get('count', len(parsed.get('data', [])))} symbols ({byte_len} B)")
                for item in parsed.get("data", []):
                    sym = item.get("symbol")
                    b = item.get("buy")
                    s = item.get("sell")
                    h = item.get("high_24h")
                    l = item.get("low_24h")
                    c = item.get("change_24h")
                    t_str = time.strftime("%H:%M:%S")
                    print(f"{t_str:<10} | {sym:<8} | {b:<10.5g} | {s:<10.5g} | {h:<10.5g} | {l:<10.5g} | {c:<+10.4f}% | {byte_len:<6}")
                print("-" * 90)
                continue

            if msg_type == "update":
                count += 1
                item = parsed.get("data", {})
                sym = item.get("symbol")
                b = item.get("buy")
                s = item.get("sell")
                h = item.get("high_24h")
                l = item.get("low_24h")
                c = item.get("change_24h")
                t_str = time.strftime("%H:%M:%S")
                print(f"{t_str:<10} | {sym:<8} | {b:<10.5g} | {s:<10.5g} | {h:<10.5g} | {l:<10.5g} | {c:<+10.4f}% | {byte_len:<6}")

        print("=" * 90)
        print(f"[CLIENT] Successfully received {count} live updates. Demonstration complete.\n")


if __name__ == "__main__":
    uri = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8765"
    asyncio.run(run_client(uri=uri))
