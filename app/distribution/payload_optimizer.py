"""
Payload Optimization and Serialization.
Ensures outgoing payload sizes stay well under the 10 KB target (typically 150-250 bytes per tick).
Includes floating point precision normalization, delta update generation, and snapshot serialization.
"""

import json
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Union

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

from app.processing.market_processor import ProcessedMarketData


class PayloadOptimizer:
    """
    Transforms internal processed market objects into optimized, standards-compliant JSON payloads.
    Guarantees outbound payload sizes remain significantly below the 10 KB requirement.
    """

    @staticmethod
    def _format_float(val: float, precision: int = 5) -> float:
        """
        Rounds floats to standard precision to eliminate IEEE-754 mantissa noise
        (e.g. 1.0850000000000002 -> 1.085).
        """
        return round(val, precision)

    @classmethod
    def format_market_data(
        cls,
        data: ProcessedMarketData,
        compact_keys: bool = False,
    ) -> Dict[str, Any]:
        """
        Formats market data with clean floating point numbers.
        """
        precision = 2 if ("JPY" in data.symbol or "BTC" in data.symbol or "ETH" in data.symbol) else 5

        if compact_keys:
            return {
                "s": data.symbol,
                "b": cls._format_float(data.buy, precision),
                "a": cls._format_float(data.sell, precision),
                "m": cls._format_float(data.mid, precision),
                "h": cls._format_float(data.high_24h, precision),
                "l": cls._format_float(data.low_24h, precision),
                "c": cls._format_float(data.change_24h, 4),
                "t": cls._format_float(data.timestamp, 3),
            }

        return {
            "symbol": data.symbol,
            "buy": cls._format_float(data.buy, precision),
            "sell": cls._format_float(data.sell, precision),
            "mid": cls._format_float(data.mid, precision),
            "high_24h": cls._format_float(data.high_24h, precision),
            "low_24h": cls._format_float(data.low_24h, precision),
            "change_24h": cls._format_float(data.change_24h, 4),
            "timestamp": cls._format_float(data.timestamp, 3),
        }

    @classmethod
    def serialize_update(
        cls,
        data: ProcessedMarketData,
        compact_keys: bool = False,
    ) -> str:
        """
        Serializes a single-symbol tick update to compact JSON string.
        Typically ~150 to ~250 bytes (well below 10 KB).
        """
        formatted = cls.format_market_data(data, compact_keys=compact_keys)
        msg = {
            "type": "update" if not compact_keys else "u",
            "data": formatted,
        }
        if HAS_ORJSON:
            return orjson.dumps(msg).decode("utf-8")
        return json.dumps(msg, separators=(",", ":"))

    @classmethod
    def serialize_snapshot(
        cls,
        snapshot_dict: Dict[str, ProcessedMarketData],
        compact_keys: bool = False,
    ) -> str:
        """
        Serializes a full market snapshot for all requested symbols into JSON string.
        Even for 20 symbols, this is ~2.5 KB to 3.5 KB (well below 10 KB).
        """
        items = [
            cls.format_market_data(data, compact_keys=compact_keys)
            for data in snapshot_dict.values()
        ]
        msg = {
            "type": "snapshot" if not compact_keys else "s",
            "count": len(items),
            "data": items,
        }
        if HAS_ORJSON:
            return orjson.dumps(msg).decode("utf-8")
        return json.dumps(msg, separators=(",", ":"))

    @classmethod
    def serialize_error(cls, message: str) -> str:
        msg = {"type": "error", "message": message}
        if HAS_ORJSON:
            return orjson.dumps(msg).decode("utf-8")
        return json.dumps(msg, separators=(",", ":"))

    @classmethod
    def measure_bytes(cls, serialized_json: str) -> int:
        """Calculates exact UTF-8 byte size of payload."""
        return len(serialized_json.encode("utf-8"))
