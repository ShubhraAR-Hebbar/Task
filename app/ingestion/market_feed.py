"""
External Market Feed Ingestion Client.
Connects to external WebSocket provider, performs authenticated handshake,
subscribes to symbols, and routes ticks into the processing pipeline with resilient reconnects.
"""

import asyncio
import logging
import random
import time
from typing import Callable, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from app.ingestion.authentication import FeedAuthenticator
from app.ingestion.subscriptions import ExternalSubscriptionBuilder
from app.monitoring.metrics import metrics

logger = logging.getLogger("pricing_engine.ingestion.feed")


class ExternalMarketFeed:
    """
    Manages the upstream WebSocket connection to the external market data provider.
    Enforces authentication verification before subscribing, and exponential backoff with jitter on disconnect.
    """

    def __init__(
        self,
        ws_url: str,
        api_key: str,
        api_secret: str,
        symbols: List[str],
        on_tick_received: Callable[[str], None],
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        timeout: float = 10.0,
    ):
        self.ws_url = ws_url
        self.authenticator = FeedAuthenticator(api_key=api_key, api_secret=api_secret)
        self.sub_builder = ExternalSubscriptionBuilder(symbols=symbols)
        self.on_tick_received = on_tick_received
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.timeout = timeout

        self._running: bool = False
        self._feed_task: Optional[asyncio.Task] = None
        self.is_connected: bool = False
        self.is_authenticated: bool = False
        self.reconnect_attempts: int = 0

    async def start(self) -> None:
        """Starts the background ingestion loop."""
        self._running = True
        self._feed_task = asyncio.create_task(self._run_feed_loop())
        logger.info(f"External Market Feed initialized for endpoint: {self.ws_url}")

    async def stop(self) -> None:
        """Stops the ingestion client gracefully."""
        self._running = False
        if self._feed_task and not self._feed_task.done():
            self._feed_task.cancel()
            try:
                await self._feed_task
            except asyncio.CancelledError:
                pass
        self.is_connected = False
        self.is_authenticated = False
        logger.info("External Market Feed stopped.")

    async def _run_feed_loop(self) -> None:
        """Persistent connection loop with backoff and jitter."""
        while self._running:
            if not self.authenticator.has_credentials:
                logger.info(
                    "[STANDBY] No external API credentials configured (API_KEY/API_SECRET empty in .env). "
                    "External live feed is in standby mode. Provide credentials in .env to connect to livefxhub."
                )
                # Sleep periodically waiting for credentials or termination
                while self._running and not self.authenticator.has_credentials:
                    await asyncio.sleep(10.0)
                if not self._running:
                    break

            try:
                await self._connect_and_stream()
                # If connection closed normally, reset backoff
                self.reconnect_attempts = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.reconnect_attempts += 1
                delay = self._compute_backoff_delay(self.reconnect_attempts)
                logger.warning(
                    f"External WebSocket error ({e}). Attempt {self.reconnect_attempts}. "
                    f"Reconnecting in {delay:.2f}s..."
                )
                await asyncio.sleep(delay)

    def _compute_backoff_delay(self, attempt: int) -> float:
        """Exponential backoff with randomized jitter (0.8x - 1.2x)."""
        base_delay = min(self.max_delay, self.initial_delay * (2 ** (attempt - 1)))
        jitter = random.uniform(0.8, 1.2)
        return base_delay * jitter

    async def _connect_and_stream(self) -> None:
        """Establishes connection, verifies authentication, subscribes, and reads frames."""
        logger.info(f"Connecting to external market data feed at {self.ws_url}...")

        async with websockets.connect(
            self.ws_url,
            open_timeout=self.timeout,
            close_timeout=5.0,
            ping_interval=20.0,
            ping_timeout=20.0,
        ) as ws:
            self.is_connected = True
            logger.info("Connected to external WebSocket. Performing authentication...")

            # 1. Send authentication message
            auth_payload = self.authenticator.create_auth_payload()
            await ws.send(auth_payload)

            # 2. Await and strictly verify authentication response
            try:
                auth_resp_raw = await asyncio.wait_for(ws.recv(), timeout=self.timeout)
            except asyncio.TimeoutError:
                raise TimeoutError("Timed out waiting for authentication response from provider.")

            is_ok, reason = self.authenticator.evaluate_auth_response(auth_resp_raw)
            if not is_ok:
                self.is_authenticated = False
                logger.error(f"Authentication failed: {reason}")
                # Close connection and trigger backoff
                await ws.close(code=1008, reason="Authentication failed")
                return

            self.is_authenticated = True
            logger.info(f"Authentication verified successfully: {reason}")

            # 3. Subscribe to market price symbols
            sub_payload = self.sub_builder.create_subscription_payload()
            await ws.send(sub_payload)
            logger.info(f"Sent subscribeMarketPrice for symbols: {self.sub_builder.symbols}")

            # 4. Message ingestion loop
            async for raw_message in ws:
                if not self._running:
                    break
                try:
                    # Ignore provider subscription acks or heartbeat frames
                    if isinstance(raw_message, (str, bytes)) and ("subscribeMarketPrice" in str(raw_message) or '"action":"auth"' in str(raw_message)):
                        continue
                    self.on_tick_received(raw_message)
                except Exception as ex:
                    logger.warning(f"Error processing incoming message frame: {ex}")
