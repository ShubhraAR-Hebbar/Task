"""
Crash Recovery and State Hydration.
Restores in-memory rolling windows, monotonic deques, and reference prices from Redis on startup.
"""

import logging
import time
from typing import List, Set

from app.processing.market_processor import MarketProcessor
from app.storage.redis_store import RedisStore

logger = logging.getLogger("pricing_engine.recovery")


class StateRecoveryManager:
    """
    Coordinates state hydration from persistent storage on process startup.
    Ensures zero loss of 24h statistical baselines after service restart.
    """

    def __init__(
        self,
        processor: MarketProcessor,
        redis_store: RedisStore,
        window_seconds: float = 86400.0,
    ):
        self.processor = processor
        self.redis_store = redis_store
        self.window_seconds = window_seconds

    async def restore_state(self, configured_symbols: List[str]) -> int:
        """
        Hydrates all active symbols from Redis.
        Returns total count of symbols restored.
        """
        if not self.redis_store.is_connected:
            logger.info("Storage is offline or disabled. Skipping recovery; starting with empty state.")
            return 0

        logger.info("Starting state recovery from Redis...")
        start_time = time.perf_counter()

        # Discover all symbols: union of configured symbols and symbols recorded in Redis
        stored_symbols = await self.redis_store.get_active_symbols()
        all_symbols: Set[str] = set(configured_symbols).union(stored_symbols)

        now = time.time()
        cutoff = now - self.window_seconds
        restored_symbols_count = 0
        total_observations_restored = 0

        for symbol in all_symbols:
            try:
                # 1. Fetch timestamped 24-hour tick history
                history = await self.redis_store.get_historical_ticks(
                    symbol=symbol, start_timestamp=cutoff
                )

                if history:
                    self.processor.load_historical_data(symbol, history)
                    restored_symbols_count += 1
                    total_observations_restored += len(history)
                else:
                    # If no detailed history exists, check for a latest snapshot
                    latest = await self.redis_store.get_latest(symbol)
                    if latest:
                        self.processor.process_raw_message(latest)
                        restored_symbols_count += 1
                        logger.info(f"Restored latest snapshot for {symbol} (no prior history).")
            except Exception as e:
                logger.error(f"Failed to recover state for symbol {symbol}: {e}")
                # Non-fatal: continue with remaining symbols

        elapsed = (time.perf_counter() - start_time) * 1000.0
        logger.info(
            f"Recovery complete in {elapsed:.2f}ms: "
            f"{restored_symbols_count}/{len(all_symbols)} symbols restored "
            f"({total_observations_restored} total observations)."
        )
        return restored_symbols_count
