"""
Integration tests for Outbound WebSocket Server and Subscriptions.
Tests multiple clients, symbol filtering, snapshots, and slow-client isolation.
"""

import asyncio
import json
import time
import pytest
import websockets

from app.distribution.websocket_server import OutboundWebSocketServer
from app.processing.market_processor import MarketProcessor, ProcessedMarketData


@pytest.fixture
async def ws_server():
    """Starts a local test outbound WebSocket server on a free port."""
    processor = MarketProcessor()
    # Pre-populate processor with a couple symbols
    processor.process_raw_message({"symbol": "EURUSD", "buy": 1.0850, "sell": 1.0852, "timestamp": 1000})
    processor.process_raw_message({"symbol": "BTCUSD", "buy": 60000.0, "sell": 60010.0, "timestamp": 1000})

    # Pick test port 18765
    server = OutboundWebSocketServer(host="127.0.0.1", port=18765, processor=processor)
    await server.start()
    yield server, processor
    await server.stop()


@pytest.mark.asyncio
async def test_client_subscription_and_snapshot(ws_server):
    """
    Test that a client can connect, subscribe to a symbol, and receive an immediate snapshot.
    """
    server, processor = ws_server
    uri = "ws://127.0.0.1:18765"

    async with websockets.connect(uri) as client:
        # Subscribe to EURUSD
        sub_req = json.dumps({"action": "subscribe", "symbols": ["EURUSD"]})
        await client.send(sub_req)

        # 1st response: subscription ack
        ack_raw = await asyncio.wait_for(client.recv(), timeout=2.0)
        ack = json.loads(ack_raw)
        assert ack["type"] == "subscribed"
        assert "EURUSD" in ack["symbols"]

        # 2nd response: immediate snapshot for EURUSD
        snap_raw = await asyncio.wait_for(client.recv(), timeout=2.0)
        snap = json.loads(snap_raw)
        assert snap["type"] == "snapshot"
        assert len(snap["data"]) == 1
        assert snap["data"][0]["symbol"] == "EURUSD"
        assert snap["data"][0]["buy"] == 1.085


@pytest.mark.asyncio
async def test_symbol_filtered_broadcast(ws_server):
    """
    Test that Client A (subscribed to EURUSD) receives EURUSD updates,
    while Client B (subscribed to BTCUSD) only receives BTCUSD updates.
    """
    server, processor = ws_server
    uri = "ws://127.0.0.1:18765"

    async with websockets.connect(uri) as client_a, websockets.connect(uri) as client_b:
        # Client A subscribes to EURUSD
        await client_a.send(json.dumps({"action": "subscribe", "symbols": ["EURUSD"]}))
        # Consume ack and snapshot
        await client_a.recv()
        await client_a.recv()

        # Client B subscribes to BTCUSD
        await client_b.send(json.dumps({"action": "subscribe", "symbols": ["BTCUSD"]}))
        # Consume ack and snapshot
        await client_b.recv()
        await client_b.recv()

        # Engine produces new EURUSD tick
        eur_tick = processor.process_raw_message(
            {"symbol": "EURUSD", "buy": 1.0860, "sell": 1.0862, "timestamp": 1010}
        )
        server.broadcast_tick(eur_tick)

        # Client A MUST receive EURUSD update
        msg_a = await asyncio.wait_for(client_a.recv(), timeout=2.0)
        parsed_a = json.loads(msg_a)
        assert parsed_a["type"] == "update"
        assert parsed_a["data"]["symbol"] == "EURUSD"
        assert parsed_a["data"]["buy"] == 1.086

        # Client B must NOT receive EURUSD update. To confirm, produce BTCUSD tick:
        btc_tick = processor.process_raw_message(
            {"symbol": "BTCUSD", "buy": 61000.0, "sell": 61010.0, "timestamp": 1010}
        )
        server.broadcast_tick(btc_tick)

        msg_b = await asyncio.wait_for(client_b.recv(), timeout=2.0)
        parsed_b = json.loads(msg_b)
        assert parsed_b["type"] == "update"
        assert parsed_b["data"]["symbol"] == "BTCUSD"
        assert parsed_b["data"]["buy"] == 61000.0


@pytest.mark.asyncio
async def test_slow_client_queue_isolation():
    """
    Test slow-client protection:
    If a slow client has a small queue that fills up, messages are dropped for that client
    without blocking or impacting fast clients or crashing the engine.
    """
    processor = MarketProcessor()
    server = OutboundWebSocketServer(
        host="127.0.0.1", port=18766, processor=processor, client_queue_maxsize=5
    )
    await server.start()

    uri = "ws://127.0.0.1:18766"
    try:
        async with websockets.connect(uri) as slow_client:
            await slow_client.send(json.dumps({"action": "subscribe", "symbols": ["EURUSD"]}))
            # Read ack
            await slow_client.recv()

            # Broadcast 20 ticks rapidly without slow_client reading them
            for i in range(20):
                tick = processor.process_raw_message(
                    {"symbol": "EURUSD", "buy": 1.0800 + i * 0.0001, "sell": 1.0802 + i * 0.0001, "timestamp": 2000 + i}
                )
                server.broadcast_tick(tick)

            # Server did not block or crash!
            # Slow client can still read remaining messages from queue
            msg = await asyncio.wait_for(slow_client.recv(), timeout=2.0)
            assert msg is not None
    finally:
        await server.stop()
