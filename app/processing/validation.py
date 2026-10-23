"""
Market data payload validation and normalization.
Extracts symbols, prices, and timestamps while strictly ignoring external 24h stats.
"""

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Union

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

logger = logging.getLogger("pricing_engine.validation")


@dataclass(slots=True)
class RawTick:
    """
    Validated and normalized raw market tick.
    """
    symbol: str
    buy: float
    sell: float
    mid: float
    timestamp: float  # Unix epoch in seconds (float precision)
    raw_payload_bytes: int = 0


class DataValidator:
    """
    Validates incoming market messages, ensuring resilience against malformed inputs,
    nulls, unexpected schemas, out-of-range prices, and timestamp variations.
    """

    def __init__(self, price_spike_threshold_pct: float = 50.0):
        self.price_spike_threshold_pct = price_spike_threshold_pct

    def parse_payload(self, raw_data: Union[str, bytes, Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], int]:
        """
        Parses raw string or bytes into a dictionary, recording byte size.
        """
        if isinstance(raw_data, dict):
            return raw_data, len(str(raw_data).encode("utf-8"))

        if isinstance(raw_data, (bytes, bytearray)):
            raw_bytes = bytes(raw_data)
        elif isinstance(raw_data, str):
            raw_bytes = raw_data.encode("utf-8")
        else:
            logger.warning(f"Unsupported payload input type: {type(raw_data)}")
            return None, 0

        byte_len = len(raw_bytes)

        try:
            if HAS_ORJSON:
                parsed = orjson.loads(raw_bytes)
            else:
                parsed = json.loads(raw_bytes)
            if not isinstance(parsed, dict):
                logger.warning(f"Parsed JSON is not an object/dict: {type(parsed)}")
                return None, byte_len
            return parsed, byte_len
        except Exception as e:
            logger.warning(f"Failed to parse JSON payload ({byte_len} bytes): {e}")
            return None, byte_len

    def validate_and_normalize(
        self,
        payload: Dict[str, Any],
        raw_size: int = 0,
        last_known_mid: Optional[float] = None
    ) -> Optional[RawTick]:
        """
        Validates required fields, normalizes types and timestamps, and returns RawTick.
        Strictly ignores incoming `low_24h`, `high_24h`, and `change_24h`.
        """
        if not isinstance(payload, dict):
            logger.warning(f"Payload is not a dictionary: {type(payload)}")
            return None

        # Extract symbol
        symbol_raw = payload.get("symbol") or payload.get("s") or payload.get("pair")
        if not symbol_raw or not isinstance(symbol_raw, str):
            logger.warning(f"Missing or invalid symbol in tick: {payload}")
            return None
        symbol = symbol_raw.strip().upper()

        # Extract buy/bid and sell/ask
        buy_val = payload.get("buy")
        if buy_val is None:
            buy_val = payload.get("bid") or payload.get("b")

        sell_val = payload.get("sell")
        if sell_val is None:
            sell_val = payload.get("ask") or payload.get("a")

        if buy_val is None or sell_val is None:
            logger.warning(f"Missing buy/sell prices for symbol {symbol}: {payload}")
            return None

        try:
            buy = float(buy_val)
            sell = float(sell_val)
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid numeric prices for symbol {symbol}: buy={buy_val}, sell={sell_val} ({e})")
            return None

        # Validate price ranges
        if buy <= 0.0 or sell <= 0.0:
            logger.warning(f"Non-positive prices encountered for {symbol}: buy={buy}, sell={sell}")
            return None

        if sell < buy:
            # In inverted crossed-market anomaly, mid is still well-defined, but log warning
            logger.warning(f"Crossed market (sell < buy) for {symbol}: buy={buy}, sell={sell}")

        # Compute consistent price basis: mid-price
        mid = (buy + sell) / 2.0

        # Anomaly detection: price spike against last known mid
        if last_known_mid is not None and last_known_mid > 0:
            pct_jump = abs(mid - last_known_mid) / last_known_mid * 100.0
            if pct_jump >= self.price_spike_threshold_pct:
                logger.warning(
                    f"Price spike anomaly detected for {symbol}: "
                    f"prev_mid={last_known_mid}, new_mid={mid} ({pct_jump:.2f}% jump). "
                    f"Processing update as legitimate market volatility."
                )

        # Extract and normalize timestamp to epoch seconds
        raw_ts = payload.get("timestamp") or payload.get("t") or payload.get("time")
        timestamp = self._parse_timestamp(raw_ts)

        return RawTick(
            symbol=symbol,
            buy=buy,
            sell=sell,
            mid=mid,
            timestamp=timestamp,
            raw_payload_bytes=raw_size,
        )

    def _parse_timestamp(self, ts: Any) -> float:
        """
        Converts timestamp in milliseconds, seconds, or ISO string to float epoch seconds.
        Falls back to current system time if missing or completely unparseable.
        """
        if ts is None:
            return time.time()

        if isinstance(ts, (int, float)):
            # If timestamp is in milliseconds (> 10^11, e.g. 1700000000000)
            if ts > 1e11:
                return float(ts) / 1000.0
            # If timestamp is in microseconds (> 10^14)
            if ts > 1e14:
                return float(ts) / 1000000.0
            # Otherwise epoch seconds
            return float(ts)

        if isinstance(ts, str):
            ts_str = ts.strip()
            # Try numeric string first
            try:
                num = float(ts_str)
                return self._parse_timestamp(num)
            except ValueError:
                pass

            # Try ISO 8601 parsing
            try:
                # Handle trailing Z
                if ts_str.endswith("Z"):
                    ts_str = ts_str[:-1] + "+00:00"
                dt = datetime.fromisoformat(ts_str)
                return dt.timestamp()
            except Exception as e:
                logger.warning(f"Could not parse ISO timestamp '{ts}': {e}. Using current time.")
                return time.time()

        logger.warning(f"Unknown timestamp type {type(ts)}: {ts}. Using current time.")
        return time.time()
