"""
Unit and integration tests for Fault Tolerance and Ingestion Resilience.
Covers external feed authentication verification, reconnect backoff, and mock feed lifecycle.
"""

import asyncio
import json
import pytest
import websockets

from app.ingestion.authentication import FeedAuthenticator
from app.ingestion.subscriptions import ExternalSubscriptionBuilder
from app.ingestion.market_feed import ExternalMarketFeed


def test_auth_payload_formatting():
    """Verify exact JSON payload required by external provider."""
    auth = FeedAuthenticator(api_key="PUBLIC_KEY_123", api_secret="SECRET_456")
    payload = json.loads(auth.create_auth_payload())

    assert payload["action"] == "auth"
    assert payload["api_key"] == "PUBLIC_KEY_123"
    assert payload["secret"] == "SECRET_456"


def test_auth_response_evaluations():
    """Verify strict authentication confirmation rules."""
    auth = FeedAuthenticator(api_key="k", api_secret="s")

    # Success scenarios
    assert auth.evaluate_auth_response({"status": "success"})[0] is True
    assert auth.evaluate_auth_response({"authenticated": True})[0] is True
    assert auth.evaluate_auth_response({"action": "auth", "status": "authenticated"})[0] is True
    assert auth.evaluate_auth_response('{"status": "ok"}')[0] is True

    # Failure scenarios
    assert auth.evaluate_auth_response({"error": "Invalid API key"})[0] is False
    assert auth.evaluate_auth_response({"event": "error", "message": "invalid credentials"})[0] is False
    assert auth.evaluate_auth_response({"status": "unauthorized"})[0] is False
    assert auth.evaluate_auth_response({"authenticated": False})[0] is False
    assert auth.evaluate_auth_response({"status": "error", "message": "Bad token"})[0] is False
    assert auth.evaluate_auth_response("NOT JSON")[0] is False


def test_subscription_payload():
    """Verify subscription format subscribeMarketPrice."""
    builder = ExternalSubscriptionBuilder(symbols=["EURUSD", "BTCUSD", "ethusd"])
    payload = json.loads(builder.create_subscription_payload())

    assert payload["action"] == "subscribeMarketPrice"
    assert payload["symbols"] == ["EURUSD", "BTCUSD", "ETHUSD"]


def test_backoff_delay_calculation():
    """Verify exponential backoff calculation with jitter bounds."""
    feed = ExternalMarketFeed(
        ws_url="ws://dummy",
        api_key="k",
        api_secret="s",
        symbols=["EURUSD"],
        on_tick_received=lambda x: None,
        initial_delay=1.0,
        max_delay=30.0,
    )

    d1 = feed._compute_backoff_delay(1)  # base ~ 1.0 * (0.8 - 1.2)
    assert 0.8 <= d1 <= 1.2

    d2 = feed._compute_backoff_delay(2)  # base ~ 2.0 * (0.8 - 1.2)
    assert 1.6 <= d2 <= 2.4

    d5 = feed._compute_backoff_delay(10)  # capped at max_delay 30.0
    assert 24.0 <= d5 <= 36.0


@pytest.mark.asyncio
async def test_mock_external_provider_flow():
    """
    Spawns a mock external provider that accepts authentication,
    receives subscribeMarketPrice, and sends a price tick.
    Verifies ExternalMarketFeed completes the full handshake.
    """
    received_auth = asyncio.Event()
    received_sub = asyncio.Event()
    received_tick = asyncio.Event()

    async def mock_provider_handler(ws):
        # 1. Wait for auth message
        msg1 = await ws.recv()
        data1 = json.loads(msg1)
        if data1.get("action") == "auth" and data1.get("api_key") == "TEST_KEY":
            received_auth.set()
            await ws.send(json.dumps({"status": "success", "message": "Authenticated"}))

        # 2. Wait for subscription
        msg2 = await ws.recv()
        data2 = json.loads(msg2)
        if data2.get("action") == "subscribeMarketPrice":
            received_sub.set()
            # 3. Stream mock tick
            await ws.send(json.dumps({
                "symbol": "EURUSD",
                "buy": 1.0850,
                "sell": 1.0852,
                "timestamp": 1700000000
            }))

    server = await websockets.serve(mock_provider_handler, "127.0.0.1", 19999)

    ticks_captured = []

    def on_tick(tick_raw):
        ticks_captured.append(tick_raw)
        received_tick.set()

    feed = ExternalMarketFeed(
        ws_url="ws://127.0.0.1:19999",
        api_key="TEST_KEY",
        api_secret="TEST_SECRET",
        symbols=["EURUSD"],
        on_tick_received=on_tick,
        timeout=2.0,
    )

    try:
        await feed.start()
        await asyncio.wait_for(received_auth.wait(), timeout=3.0)
        await asyncio.wait_for(received_sub.wait(), timeout=3.0)
        await asyncio.wait_for(received_tick.wait(), timeout=3.0)

        assert feed.is_authenticated is True
        assert len(ticks_captured) == 1
        assert "EURUSD" in ticks_captured[0]
    finally:
        await feed.stop()
        server.close()
        await server.wait_closed()
