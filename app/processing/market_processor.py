"""
Central Market Data Processor.
Orchestrates validation, rolling window calculations, and per-symbol state caching.
"""

import time
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from app.monitoring.metrics import metrics
from app.processing.rolling_window import RollingWindow, WindowStats
from app.processing.validation import DataValidator, RawTick

logger = logging.getLogger("pricing_engine.processor")


@dataclass(slots=True)
class ProcessedMarketData:
    """
    Standardized processed market data emitted by the engine.
    """
    symbol: str
    buy: float
    sell: float
    mid: float
    high_24h: float
    low_24h: float
    change_24h: float
    timestamp: float
    calculated_at: float
    observation_count: int
    is_changed: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "buy": self.buy,
            "sell": self.sell,
            "mid": self.mid,
            "high_24h": self.high_24h,
            "low_24h": self.low_24h,
            "change_24h": self.change_24h,
            "timestamp": self.timestamp,
            "calculated_at": self.calculated_at,
        }


class MarketProcessor:
    """
    Stateful processor maintaining independent rolling windows for all tracked symbols.
    """

    def __init__(self, window_seconds: float = 86400.0, price_spike_threshold_pct: float = 50.0):
        self.window_seconds = window_seconds
        self.validator = DataValidator(price_spike_threshold_pct=price_spike_threshold_pct)
        self.windows: Dict[str, RollingWindow] = {}
        self.latest_data: Dict[str, ProcessedMarketData] = {}

    def get_window(self, symbol: str) -> RollingWindow:
        if symbol not in self.windows:
            self.windows[symbol] = RollingWindow(window_seconds=self.window_seconds)
            metrics.set_active_symbols(len(self.windows))
        return self.windows[symbol]

    def process_raw_message(self, raw_data: Union[str, bytes, Dict[str, Any]]) -> Optional[ProcessedMarketData]:
        """
        End-to-end processing of an incoming message from the feed:
        1. Parsing JSON & measuring inbound payload size
        2. Validating types & ignoring provider 24h stats
        3. Updating rolling 24h window (O(1) amortized)
        4. Instrumenting processing latency
        """
        t_start = time.perf_counter()
        metrics.record_received()

        # Step 1: Parse JSON
        parsed, byte_size = self.validator.parse_payload(raw_data)
        if parsed is None:
            metrics.record_malformed()
            return None

        symbol_hint = str(parsed.get("symbol") or parsed.get("s") or "").strip().upper()
        last_mid = self.latest_data[symbol_hint].mid if symbol_hint in self.latest_data else None

        # Step 2: Validate and extract normalized fields
        raw_tick = self.validator.validate_and_normalize(
            payload=parsed,
            raw_size=byte_size,
            last_known_mid=last_mid,
        )
        if raw_tick is None:
            metrics.record_malformed()
            return None

        # Step 3: Update 24-hour Rolling Window
        window = self.get_window(raw_tick.symbol)
        stats = window.add_observation(
            timestamp=raw_tick.timestamp,
            buy=raw_tick.buy,
            sell=raw_tick.sell,
            mid=raw_tick.mid,
        )

        now_calc = time.time()

        # Check if values actually changed compared to prior emission
        prior = self.latest_data.get(raw_tick.symbol)
        is_changed = True
        if prior is not None:
            is_changed = not (
                prior.buy == stats.buy
                and prior.sell == stats.sell
                and prior.high_24h == stats.high_24h
                and prior.low_24h == stats.low_24h
                and prior.change_24h == stats.change_24h
            )

        processed = ProcessedMarketData(
            symbol=raw_tick.symbol,
            buy=stats.buy,
            sell=stats.sell,
            mid=stats.mid,
            high_24h=stats.high_24h,
            low_24h=stats.low_24h,
            change_24h=stats.change_24h,
            timestamp=stats.timestamp,
            calculated_at=now_calc,
            observation_count=stats.observation_count,
            is_changed=is_changed,
        )

        self.latest_data[raw_tick.symbol] = processed

        # Step 4: Record processing latency
        t_proc = time.perf_counter() - t_start
        metrics.record_processing_latency(t_proc)

        return processed

    def get_snapshot(self, symbols: Optional[List[str]] = None) -> Dict[str, ProcessedMarketData]:
        """
        Retrieves snapshot of latest market data for specified or all active symbols.
        """
        if symbols is None:
            return dict(self.latest_data)
        upper_symbols = {s.upper() for s in symbols}
        return {sym: data for sym, data in self.latest_data.items() if sym in upper_symbols}

    def load_historical_data(self, symbol: str, history: List[Tuple[float, float, float, float]]) -> None:
        """
        Restores historical ticks for a symbol from durable storage.
        """
        window = self.get_window(symbol)
        window.load_history(history)
        if window.latest_stats is not None:
            stats = window.latest_stats
            self.latest_data[symbol] = ProcessedMarketData(
                symbol=symbol,
                buy=stats.buy,
                sell=stats.sell,
                mid=stats.mid,
                high_24h=stats.high_24h,
                low_24h=stats.low_24h,
                change_24h=stats.change_24h,
                timestamp=stats.timestamp,
                calculated_at=time.time(),
                observation_count=stats.observation_count,
                is_changed=True,
            )
            logger.info(f"Restored {stats.observation_count} observations for {symbol} into 24h rolling window.")
