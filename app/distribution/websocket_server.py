"""
Outbound WebSocket Distribution Server.
Serves real-time market data, handles subscriptions, and enforces slow-client isolation.
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Set

import websockets
try:
    from websockets.asyncio.server import ServerConnection as WebSocketConnection
except ImportError:
    from websockets.server import WebSocketServerProtocol as WebSocketConnection

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

from app.distribution.payload_optimizer import PayloadOptimizer
from app.distribution.subscriptions import SubscriptionManager
from app.monitoring.metrics import metrics
from app.processing.market_processor import MarketProcessor, ProcessedMarketData

logger = logging.getLogger("pricing_engine.distribution.server")


class OutboundWebSocketServer:
    """
    Distributes processed market updates to connected downstream clients.
    Features:
    - Fine-grained symbol subscriptions.
    - Non-blocking broadcast.
    - Slow-client protection via bounded per-client queues.
    - Zero interference between slow and fast clients.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8765,
        processor: Optional[MarketProcessor] = None,
        client_queue_maxsize: int = 1000,
    ):
        self.host = host
        self.port = port
        self.processor = processor
        self.subscription_mgr = SubscriptionManager(default_queue_size=client_queue_maxsize)
        self.server: Optional[Any] = None
        self._running: bool = False
        self._connected_sockets: Dict[str, WebSocketConnection] = {}

    async def start(self) -> None:
        """Starts the outbound WebSocket server."""
        self._running = True
        self.server = await websockets.serve(
            self._handle_client,
            self.host,
            self.port,
            ping_interval=20.0,
            ping_timeout=20.0,
        )
        logger.info(f"Outbound WebSocket Server listening on ws://{self.host}:{self.port}")

    async def stop(self) -> None:
        """Gracefully closes server and all client connections."""
        self._running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        # Close all active client connections
        sockets = list(self._connected_sockets.values())
        for ws in sockets:
            try:
                await ws.close(code=1001, reason="Server shutting down")
            except Exception:
                pass
        self._connected_sockets.clear()
        logger.info("Outbound WebSocket Server stopped.")

    async def _handle_client(self, websocket: WebSocketConnection) -> None:
        """Lifecycle handler for a new client connection."""
        client_id = f"client-{uuid.uuid4().hex[:8]}"
        self._connected_sockets[client_id] = websocket
        queue = self.subscription_mgr.register_client(client_id)
        metrics.set_active_clients(self.subscription_mgr.client_count)

        writer_task = asyncio.create_task(self._client_writer(client_id, websocket, queue))
        try:
            await self._client_reader(client_id, websocket, queue)
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {client_id} connection closed.")
        except Exception as e:
            logger.warning(f"Error handling client {client_id}: {e}")
        finally:
            writer_task.cancel()
            self._connected_sockets.pop(client_id, None)
            self.subscription_mgr.unregister_client(client_id)
            metrics.set_active_clients(self.subscription_mgr.client_count)

    async def _client_reader(
        self,
        client_id: str,
        websocket: WebSocketConnection,
        queue: asyncio.Queue,
    ) -> None:
        """Receives and executes client subscription requests."""
        async for message in websocket:
            try:
                if HAS_ORJSON:
                    msg = orjson.loads(message)
                else:
                    msg = json.loads(message)
            except Exception as e:
                err_payload = PayloadOptimizer.serialize_error(f"Malformed JSON: {e}")
                self._safe_enqueue(queue, err_payload)
                continue

            action = msg.get("action") or msg.get("type")
            if not action:
                self._safe_enqueue(
                    queue, PayloadOptimizer.serialize_error("Missing 'action' field in message.")
                )
                continue

            action_lower = str(action).lower()

            if action_lower == "subscribe":
                symbols = msg.get("symbols") or []
                if isinstance(symbols, str):
                    symbols = [symbols]
                newly_added = self.subscription_mgr.subscribe(client_id, symbols)

                # Send subscription confirmation
                ack = json.dumps({"type": "subscribed", "symbols": list(newly_added)})
                self._safe_enqueue(queue, ack)

                # Immediate snapshot for newly subscribed symbols
                if self.processor and newly_added:
                    snapshot = self.processor.get_snapshot(list(newly_added))
                    if snapshot:
                        snap_payload = PayloadOptimizer.serialize_snapshot(snapshot)
                        self._safe_enqueue(queue, snap_payload)

            elif action_lower == "unsubscribe":
                symbols = msg.get("symbols") or []
                if isinstance(symbols, str):
                    symbols = [symbols]
                removed = self.subscription_mgr.unsubscribe(client_id, symbols)
                ack = json.dumps({"type": "unsubscribed", "symbols": list(removed)})
                self._safe_enqueue(queue, ack)

            elif action_lower == "snapshot":
                symbols = msg.get("symbols")
                if isinstance(symbols, str):
                    symbols = [symbols]
                if self.processor:
                    snapshot = self.processor.get_snapshot(symbols)
                    snap_payload = PayloadOptimizer.serialize_snapshot(snapshot)
                    self._safe_enqueue(queue, snap_payload)

            elif action_lower == "ping":
                self._safe_enqueue(queue, json.dumps({"type": "pong", "t": time.time()}))

            else:
                self._safe_enqueue(
                    queue, PayloadOptimizer.serialize_error(f"Unknown action: '{action}'")
                )

    async def _client_writer(
        self,
        client_id: str,
        websocket: WebSocketConnection,
        queue: asyncio.Queue,
    ) -> None:
        """Pulls messages from the client's queue and sends across the WebSocket."""
        try:
            while self._running:
                payload = await queue.get()
                await websocket.send(payload)
                queue.task_done()
        except asyncio.CancelledError:
            pass
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.warning(f"Writer exception for {client_id}: {e}")

    def _safe_enqueue(self, queue: asyncio.Queue, payload: str) -> bool:
        """
        Pushes to queue non-blockingly. If full, drops oldest item (slow-client protection).
        """
        try:
            queue.put_nowait(payload)
            return True
        except asyncio.QueueFull:
            metrics.record_dropped()
            try:
                # Evict oldest item to prevent memory bloat and prioritize latest prices
                queue.get_nowait()
                queue.task_done()
                queue.put_nowait(payload)
                return True
            except Exception:
                return False

    def broadcast_tick(self, data: ProcessedMarketData) -> int:
        """
        Fast broadcast to subscribed clients.
        Guarantees O(subscribers) and zero blocking on network I/O.
        Returns number of clients reached.
        """
        subscribers = self.subscription_mgr.get_subscribers_for_symbol(data.symbol)
        if not subscribers:
            return 0

        t_start = time.perf_counter()

        # Serialize payload once for all subscribers of this symbol
        payload = PayloadOptimizer.serialize_update(data)
        byte_len = PayloadOptimizer.measure_bytes(payload)
        metrics.record_payload_size(byte_len)

        delivered_count = 0
        for client_id in subscribers:
            session = self.subscription_mgr.get_client_session(client_id)
            if session:
                success = self._safe_enqueue(session.queue, payload)
                if success:
                    delivered_count += 1
                else:
                    session.dropped_count += 1
                    if session.dropped_count > 50:
                        # Severely lagging client: disconnect to protect server
                        logger.warning(
                            f"Disconnecting lagging slow client {client_id} "
                            f"(dropped {session.dropped_count} messages)."
                        )
                        ws = self._connected_sockets.get(client_id)
                        if ws:
                            asyncio.create_task(
                                ws.close(code=1008, reason="Slow client buffer overflow")
                            )

        t_dist = time.perf_counter() - t_start
        metrics.record_distribution_latency(t_dist, client_count=delivered_count)
        return delivered_count
