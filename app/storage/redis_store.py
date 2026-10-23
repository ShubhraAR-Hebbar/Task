"""
Redis State Storage with Resilience and Automatic Failover.
Persists latest prices and timestamped 24-hour history for crash recovery.
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import redis.asyncio as aioredis
    from redis.exceptions import ConnectionError, RedisError, TimeoutError
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

from app.processing.market_processor import ProcessedMarketData

logger = logging.getLogger("pricing_engine.storage")


class RedisStore:
    """
    Asynchronous Redis client managing state persistence with graceful fallback.
    If Redis is unavailable, engine continues running without blocking WebSocket traffic.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        enabled: bool = True,
        retention_seconds: int = 90000,
        retry_interval: float = 5.0,
    ):
        self.redis_url = redis_url
        self.enabled = enabled and HAS_REDIS
        self.retention_seconds = retention_seconds
        self.retry_interval = retry_interval

        self.client: Optional[Any] = None
        self.is_connected: bool = False
        self._reconnect_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._write_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._writer_task: Optional[asyncio.Task] = None
        self._running: bool = False

    async def initialize(self, custom_client: Optional[Any] = None) -> bool:
        """
        Initializes Redis connection and launches background persistence workers.
        """
        self._running = True
        if not self.enabled:
            logger.info("Redis persistence is disabled. Operating in pure in-memory mode.")
            return False

        if custom_client is not None:
            # Allows passing fakeredis client for deterministic unit testing
            self.client = custom_client
            self.is_connected = True
        else:
            await self._connect()

        self._writer_task = asyncio.create_task(self._process_write_queue())
        self._cleanup_task = asyncio.create_task(self._periodic_pruning())
        return self.is_connected

    async def _connect(self) -> bool:
        try:
            self.client = aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=3.0,
                socket_connect_timeout=3.0,
            )
            await self.client.ping()
            self.is_connected = True
            logger.info(f"Connected to Redis at {self.redis_url}")
            return True
        except Exception as e:
            self.is_connected = False
            logger.warning(f"Could not connect to Redis ({e}). Running in degraded in-memory mode.")
            if not self._reconnect_task or self._reconnect_task.done():
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())
            return False

    async def _reconnect_loop(self) -> None:
        """Background loop attempting reconnection when Redis is unreachable."""
        while self._running and not self.is_connected:
            await asyncio.sleep(self.retry_interval)
            logger.info("Attempting Redis reconnection...")
            if await self._connect():
                logger.info("Successfully reconnected to Redis.")
                break

    async def persist_tick(self, data: ProcessedMarketData) -> None:
        """
        Enqueues market tick for non-blocking asynchronous persistence.
        Guarantees zero blocking of the market processing pipeline.
        """
        if not self.enabled:
            return

        try:
            self._write_queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.warning("Redis write queue is full; dropping oldest persistence task.")
            try:
                self._write_queue.get_nowait()
                self._write_queue.put_nowait(data)
            except Exception:
                pass

    async def _process_write_queue(self) -> None:
        """Worker task executing batched Redis writes."""
        while self._running:
            try:
                data: ProcessedMarketData = await self._write_queue.get()
                if not self.is_connected or not self.client:
                    self._write_queue.task_done()
                    continue

                await self._persist_to_redis(data)
                self._write_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error persisting to Redis: {e}")
                self.is_connected = False
                if not self._reconnect_task or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _persist_to_redis(self, data: ProcessedMarketData) -> None:
        sym = data.symbol
        ts = data.timestamp

        # 1. Store latest state in Hash
        latest_key = f"engine:latest:{sym}"
        latest_dict = data.to_dict()
        latest_payload = (
            orjson.dumps(latest_dict).decode("utf-8")
            if HAS_ORJSON
            else json.dumps(latest_dict)
        )

        # 2. Append tick observation to Sorted Set (ZSET)
        history_key = f"engine:history:{sym}"
        # Compact tick representation: timestamp, mid, buy, sell
        tick_entry = {
            "t": ts,
            "m": data.mid,
            "b": data.buy,
            "s": data.sell,
        }
        tick_payload = (
            orjson.dumps(tick_entry).decode("utf-8")
            if HAS_ORJSON
            else json.dumps(tick_entry)
        )

        pipe = self.client.pipeline()
        pipe.set(latest_key, latest_payload)
        pipe.zadd(history_key, {tick_payload: ts})
        pipe.sadd("engine:symbols", sym)
        await pipe.execute()

    async def get_active_symbols(self) -> Set[str]:
        """Returns all registered symbols from Redis."""
        if not self.is_connected or not self.client:
            return set()
        try:
            return set(await self.client.smembers("engine:symbols"))
        except Exception as e:
            logger.warning(f"Failed to fetch symbols from Redis: {e}")
            return set()

    async def get_historical_ticks(
        self, symbol: str, start_timestamp: float
    ) -> List[Tuple[float, float, float, float]]:
        """
        Retrieves timestamped history since start_timestamp for startup recovery.
        Returns list of (timestamp, mid, buy, sell).
        Handles corrupted entries gracefully.
        """
        if not self.is_connected or not self.client:
            return []

        history_key = f"engine:history:{symbol}"
        try:
            raw_entries = await self.client.zrangebyscore(
                history_key, min=start_timestamp, max="+inf"
            )
            ticks: List[Tuple[float, float, float, float]] = []

            for entry in raw_entries:
                try:
                    parsed = orjson.loads(entry) if HAS_ORJSON else json.loads(entry)
                    t = float(parsed["t"])
                    m = float(parsed["m"])
                    b = float(parsed["b"])
                    s = float(parsed["s"])
                    ticks.append((t, m, b, s))
                except Exception as e:
                    logger.warning(f"Corrupted tick history entry for {symbol} skipped: {entry} ({e})")
                    continue

            return ticks
        except Exception as e:
            logger.error(f"Failed to fetch historical ticks for {symbol}: {e}")
            return []

    async def get_latest(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetches latest snapshot from Redis."""
        if not self.is_connected or not self.client:
            return None
        try:
            raw = await self.client.get(f"engine:latest:{symbol}")
            if raw:
                return orjson.loads(raw) if HAS_ORJSON else json.loads(raw)
        except Exception as e:
            logger.warning(f"Failed to fetch latest for {symbol}: {e}")
        return None

    async def _periodic_pruning(self) -> None:
        """Periodically prunes ticks older than retention_seconds from Redis."""
        while self._running:
            try:
                await asyncio.sleep(300.0)  # Prune every 5 minutes
                if self.is_connected and self.client:
                    cutoff = time.time() - self.retention_seconds
                    symbols = await self.get_active_symbols()
                    for sym in symbols:
                        history_key = f"engine:history:{sym}"
                        await self.client.zremrangebyscore(history_key, min="-inf", max=cutoff)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error during Redis periodic pruning: {e}")

    async def close(self) -> None:
        """Graceful shutdown of persistence worker."""
        self._running = False
        if self._writer_task and not self._writer_task.done():
            self._writer_task.cancel()
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()

        if self.client:
            try:
                await self.client.aclose()
            except Exception:
                pass
            self.is_connected = False
            logger.info("Redis connection closed.")
