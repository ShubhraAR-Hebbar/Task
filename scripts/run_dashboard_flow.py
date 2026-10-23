"""
One-Click Real-Time Pricing Engine Flow Runner.
Coordinates:
1. Mock Market Data Provider (ws://127.0.0.1:9001)
2. Production Pricing Engine Pipeline (ws://127.0.0.1:8765)
3. HTTP Dashboard Server (http://127.0.0.1:8080)
"""

import asyncio
import functools
import http.server
import logging
import os
import socketserver
import sys
import threading
import time
import webbrowser

# Add repo root to sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from app.config import settings
from app.main import PricingEngineApp
from scripts.mock_external_feed import handle_mock_client
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("dashboard_flow")


def run_http_server(dashboard_dir: str, port: int = 8080):
    """Runs a lightweight HTTP server to host the dashboard."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=dashboard_dir)
    # Allow port reuse
    socketserver.TCPServer.allow_reuse_address = True
    try:
        with socketserver.TCPServer(("0.0.0.0", port), handler) as httpd:
            logger.info(f"Dashboard Web UI available at: http://localhost:{port}/")
            httpd.serve_forever()
    except Exception as e:
        logger.error(f"HTTP Server failed: {e}")


async def main():
    dashboard_path = os.path.join(BASE_DIR, "dashboard")
    http_port = 8080
    feed_port = 9001
    engine_ws_port = 8765

    print("\n" + "=" * 64)
    print("      REAL-TIME PRICING ENGINE - LIVE FLOW RUNNER")
    print("=" * 64)
    print(f"[*] Dashboard UI:         http://localhost:{http_port}/")
    print(f"[*] Outbound Engine WS:   ws://localhost:{engine_ws_port}")
    print(f"[*] Upstream Market Feed: ws://localhost:{feed_port}")
    print("=" * 64 + "\n")

    # 1. Start HTTP Server in background thread
    http_thread = threading.Thread(
        target=run_http_server,
        args=(dashboard_path, http_port),
        daemon=True,
    )
    http_thread.start()

    # 2. Start Mock Upstream Feed Server
    mock_server = await websockets.serve(handle_mock_client, "127.0.0.1", feed_port)
    logger.info(f"Mock Upstream Market Feed started on ws://127.0.0.1:{feed_port}")

    # 3. Configure and Start Pricing Engine
    # Point the pricing engine to our mock feed
    settings.external_ws_url = f"ws://127.0.0.1:{feed_port}"
    settings.api_key = "demo_key"
    settings.api_secret = "demo_secret"
    settings.outbound_ws_port = engine_ws_port
    settings.redis_enabled = False  # Pure in-memory O(1) low-latency mode for flow demo

    engine_app = PricingEngineApp()
    await engine_app.start()
    logger.info("Pricing Engine is actively processing ticks and distributing updates!")

    # 4. Open dashboard in default browser after 1 second
    def open_browser():
        time.sleep(1.0)
        webbrowser.open(f"http://localhost:{http_port}/")

    threading.Thread(target=open_browser, daemon=True).start()

    # 5. Keep running forever
    try:
        await asyncio.Future()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Stopping all flow services...")
        await engine_app.stop()
        mock_server.close()
        await mock_server.wait_closed()
        logger.info("Live flow runner terminated cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
