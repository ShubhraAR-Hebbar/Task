"""
Main Application Entry Point.
Coordinates ingestion, processing, state persistence, outbound distribution, and metrics reporting.
"""

import asyncio
import logging
import signal
import sys
from typing import Optional

from app.config import settings
from app.distribution.websocket_server import OutboundWebSocketServer
from app.ingestion.market_feed import ExternalMarketFeed
from app.logging_config import setup_logging
from app.monitoring.metrics import metrics
from app.processing.market_processor import MarketProcessor
from app.storage.recovery import StateRecoveryManager
from app.storage.redis_store import RedisStore

logger = logging.getLogger("pricing_engine.main")


class PricingEngineApp:
    """
    Orchestrator for the Real-Time Pricing Engine.
    """

    def __init__(self):
        self.processor: Optional[MarketProcessor] = None
        self.redis_store: Optional[RedisStore] = None
        self.recovery_mgr: Optional[StateRecoveryManager] = None
        self.outbound_server: Optional[OutboundWebSocketServer] = None
        self.external_feed: Optional[ExternalMarketFeed] = None
        self._metrics_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Boots the entire pricing engine pipeline."""
        setup_logging(log_level=settings.log_level)
        logger.info("=========================================================")
        logger.info("   Starting Production Real-Time Pricing Engine          ")
        logger.info("=========================================================")
        logger.info(f"Target Symbols: {settings.subscribe_symbols}")
        logger.info(f"Rolling Window: {settings.window_seconds}s (24 Hours)")
        logger.info(f"Outbound WebSocket: ws://{settings.outbound_ws_host}:{settings.outbound_ws_port}")
        logger.info(f"Redis Persistence: {'Enabled' if settings.redis_enabled else 'Disabled'}")

        # 1. Initialize Market Processor
        self.processor = MarketProcessor(
            window_seconds=settings.window_seconds,
            price_spike_threshold_pct=settings.price_spike_threshold_pct,
        )

        # 2. Initialize Redis Storage & Recover State
        self.redis_store = RedisStore(
            redis_url=settings.redis_url,
            enabled=settings.redis_enabled,
            retention_seconds=settings.redis_retention_seconds,
            retry_interval=settings.redis_retry_interval_seconds,
        )
        await self.redis_store.initialize()

        self.recovery_mgr = StateRecoveryManager(
            processor=self.processor,
            redis_store=self.redis_store,
            window_seconds=settings.window_seconds,
        )
        await self.recovery_mgr.restore_state(settings.subscribe_symbols)

        # 3. Start Outbound Client WebSocket Server
        self.outbound_server = OutboundWebSocketServer(
            host=settings.outbound_ws_host,
            port=settings.outbound_ws_port,
            processor=self.processor,
            client_queue_maxsize=settings.client_queue_maxsize,
        )
        await self.outbound_server.start()

        # 4. Define Tick Pipeline Callback
        loop = asyncio.get_running_loop()

        def handle_incoming_tick(raw_data: str) -> None:
            processed = self.processor.process_raw_message(raw_data)
            if processed is not None:
                # Distribute to subscribed clients
                self.outbound_server.broadcast_tick(processed)
                # Persist to Redis asynchronously
                if self.redis_store and self.redis_store.is_connected:
                    asyncio.create_task(self.redis_store.persist_tick(processed))

        # 5. Initialize & Start External Provider Market Feed
        self.external_feed = ExternalMarketFeed(
            ws_url=settings.external_ws_url,
            api_key=settings.api_key,
            api_secret=settings.api_secret,
            symbols=settings.subscribe_symbols,
            on_tick_received=handle_incoming_tick,
            initial_delay=settings.external_ws_reconnect_initial_delay,
            max_delay=settings.external_ws_reconnect_max_delay,
            timeout=settings.external_ws_timeout_seconds,
        )
        await self.external_feed.start()

        # 6. Start Periodic Metrics Reporter
        self._metrics_task = asyncio.create_task(self._periodic_metrics_reporter())

        logger.info("Pricing Engine is fully operational and ready for client connections.")

    async def _periodic_metrics_reporter(self) -> None:
        """Periodically outputs telemetry and latency metrics."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(settings.metrics_report_interval_seconds)
                metrics.log_report()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in metrics reporter: {e}")

    async def stop(self) -> None:
        """Gracefully shuts down all services."""
        logger.info("Initiating graceful shutdown...")
        self._stop_event.set()

        if self._metrics_task and not self._metrics_task.done():
            self._metrics_task.cancel()

        if self.external_feed:
            await self.external_feed.stop()

        if self.outbound_server:
            await self.outbound_server.stop()

        if self.redis_store:
            await self.redis_store.close()

        logger.info("Engine successfully shut down.")


async def main() -> None:
    engine_app = PricingEngineApp()

    # Register OS signal handlers for graceful shutdown (Unix/Windows compatible)
    loop = asyncio.get_running_loop()

    def shutdown_signal():
        logger.info("Received termination signal.")
        asyncio.create_task(engine_app.stop())

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_signal)

    try:
        await engine_app.start()
        # Keep running until stop event is triggered
        await engine_app._stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Keyboard interrupt received.")
        await engine_app.stop()
    except Exception as e:
        logger.critical(f"Fatal application error: {e}", exc_info=True)
        await engine_app.stop()
        sys.exit(1)


async def asgi_handler(scope, receive, send):
    """ASGI entrypoint for serverless environments (e.g. Vercel)."""
    if scope.get("type") == "http":
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [
                [b"content-type", b"application/json"],
            ],
        })
        await send({
            "type": "http.response.body",
            "body": b'{"status": "ok", "service": "real-time-pricing-engine", "version": "1.0.0"}',
        })

# Export top-level symbols expected by Vercel Python runtime
app = asgi_handler
handler = asgi_handler


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

