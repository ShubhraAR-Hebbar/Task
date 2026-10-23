"""
Mock External Market Data Provider.
Simulates wss://v3-prod.livefxhub.com/ws/algo for local testing, integration validation, and demos.
Enforces authentication and subscribeMarketPrice handshakes, then streams live market ticks.
"""

import asyncio
import json
import logging
import math
import os
import random
import sys
import time

# Ensure repository root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [mock_feed] %(message)s")
logger = logging.getLogger("mock_feed")

BASE_PRICES = {
    "EURUSD": 1.0850,
    "GBPUSD": 1.2950,
    "USDJPY": 154.20,
    "BTCUSD": 68500.0,
    "ETHUSD": 3520.0,
}


async def handle_mock_client(websocket):
    client_addr = websocket.remote_address
    logger.info(f"Client connected from {client_addr}")

    authenticated = False
    subscribed_symbols = []

    try:
        # Phase 1: Wait for authentication
        auth_msg_raw = await websocket.recv()
        auth_msg = json.loads(auth_msg_raw)

        if auth_msg.get("action") == "auth":
            api_key = auth_msg.get("api_key")
            secret = auth_msg.get("secret")
            logger.info(f"Received auth request with api_key={api_key[:4]}***")
            # Confirm authentication
            authenticated = True
            await websocket.send(json.dumps({
                "status": "success",
                "message": "Authentication successful",
                "authenticated": True,
            }))
        else:
            logger.warning(f"Unexpected initial message: {auth_msg}")
            await websocket.send(json.dumps({
                "status": "error",
                "message": "First message must be action='auth'",
            }))
            await websocket.close(1008)
            return

        # Phase 2: Wait for subscription
        sub_msg_raw = await websocket.recv()
        sub_msg = json.loads(sub_msg_raw)

        if sub_msg.get("action") == "subscribeMarketPrice":
            subscribed_symbols = sub_msg.get("symbols", list(BASE_PRICES.keys()))
            logger.info(f"Client subscribed to: {subscribed_symbols}")
            await websocket.send(json.dumps({
                "status": "success",
                "action": "subscribeMarketPrice",
                "symbols": subscribed_symbols,
            }))
        else:
            logger.warning(f"Expected subscribeMarketPrice, received: {sub_msg}")
            subscribed_symbols = list(BASE_PRICES.keys())

        # Phase 3: Continuous Price Ticks
        prices = {sym: BASE_PRICES.get(sym, 100.0) for sym in subscribed_symbols}
        step = 0

        while True:
            step += 1
            for sym in subscribed_symbols:
                # Random walk simulation
                volatility = 0.0002 if "USD" in sym and "BTC" not in sym else 15.0
                delta = random.gauss(0, volatility)
                prices[sym] = max(0.0001, prices[sym] + delta)

                spread = 0.0002 if prices[sym] < 10 else 2.0
                buy = prices[sym]
                sell = prices[sym] + spread
                now_ts = time.time()

                # Notice: we send dummy high_24h, low_24h, change_24h to verify the engine ignores them!
                tick = {
                    "symbol": sym,
                    "buy": round(buy, 5 if buy < 10 else 2),
                    "sell": round(sell, 5 if sell < 10 else 2),
                    "low_24h": 0.0001,    # Mock dummy
                    "high_24h": 99999.0,   # Mock dummy
                    "change_24h": 77.7,    # Mock dummy
                    "timestamp": round(now_ts * 1000),  # In milliseconds
                }
                await websocket.send(json.dumps(tick))

            await asyncio.sleep(0.1)  # 10 updates/sec per symbol

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Client {client_addr} disconnected.")
    except Exception as e:
        logger.warning(f"Error handling client {client_addr}: {e}")


async def main():
    port = int(os.environ.get("MOCK_FEED_PORT", 9001))
    server = await websockets.serve(handle_mock_client, "0.0.0.0", port)
    logger.info(f"Mock External Feed server running on ws://0.0.0.0:{port}")
    await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Mock feed terminated.")
