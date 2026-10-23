"""
Unit tests for PayloadOptimizer and payload size measurements.
Verifies compliance with the < 10 KB outbound payload requirement.
"""

import time
import pytest
from app.distribution.payload_optimizer import PayloadOptimizer
from app.processing.market_processor import ProcessedMarketData


def create_sample_tick(symbol: str = "EURUSD", buy: float = 1.08502, sell: float = 1.08518) -> ProcessedMarketData:
    return ProcessedMarketData(
        symbol=symbol,
        buy=buy,
        sell=sell,
        mid=(buy + sell) / 2.0,
        high_24h=1.09200,
        low_24h=1.08100,
        change_24h=0.1523,
        timestamp=time.time(),
        calculated_at=time.time(),
        observation_count=100,
        is_changed=True,
    )


def test_single_update_payload_size_under_10kb():
    """
    Mandatory Requirement: Serialized outbound tick payload must be < 10 KB.
    In practice, our optimized JSON is ~200 bytes.
    """
    data = create_sample_tick()
    payload = PayloadOptimizer.serialize_update(data)
    size_bytes = PayloadOptimizer.measure_bytes(payload)

    # Must be strictly under 10 KB (10,240 bytes)
    assert size_bytes < 10240
    # In fact, it should be well under 400 bytes
    assert size_bytes < 400
    assert "EURUSD" in payload
    assert "update" in payload


def test_compact_update_payload_size():
    """
    Test compact keys format (s, b, a, m, h, l, c, t) for ultra-constrained networks.
    """
    data = create_sample_tick()
    compact_payload = PayloadOptimizer.serialize_update(data, compact_keys=True)
    size_bytes = PayloadOptimizer.measure_bytes(compact_payload)

    assert size_bytes < 200  # ultra compact: ~120-160 bytes!


def test_multi_symbol_snapshot_payload_under_10kb():
    """
    Mandatory Requirement: Outbound snapshot containing all symbols must be < 10 KB.
    """
    symbols = [
        "EURUSD", "GBPUSD", "USDJPY", "BTCUSD", "ETHUSD",
        "AUDUSD", "USDCAD", "USDCHF", "NZDUSD", "EURJPY",
        "GBPJPY", "EURGBP", "XAUUSD", "XAGUSD", "SOLUSD"
    ]
    snapshot = {sym: create_sample_tick(sym) for sym in symbols}

    payload = PayloadOptimizer.serialize_snapshot(snapshot)
    size_bytes = PayloadOptimizer.measure_bytes(payload)

    # 15 symbols snapshot must be well under 10 KB
    assert size_bytes < 10240
    # Typically ~2.5 KB
    assert size_bytes < 4000
    assert len(snapshot) == 15


def test_float_precision_formatting():
    """
    Test that floating point mantissa noise (e.g. 1.0850000000000002) is removed.
    """
    # Create tick with float noise
    tick = create_sample_tick(buy=1.0850000000000002, sell=1.0852000000000001)
    formatted = PayloadOptimizer.format_market_data(tick)

    assert formatted["buy"] == 1.085
    assert formatted["sell"] == 1.0852
    assert "1.0850000000000002" not in str(formatted)
