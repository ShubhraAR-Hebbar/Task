"""
Unit tests for the MarketProcessor and validation layer.
"""

import json
import pytest
from app.processing.market_processor import MarketProcessor


def test_ignores_external_24h_fields():
    """
    Mandatory Requirement: Strictly ignore incoming low_24h, high_24h, change_24h
    and compute them independently.
    """
    processor = MarketProcessor(window_seconds=86400.0)

    # Inbound tick with deceptive fake 24h stats
    tick = {
        "symbol": "EURUSD",
        "buy": 1.1000,
        "sell": 1.1010,
        "low_24h": 0.5000,   # FAKE external value, MUST BE IGNORED
        "high_24h": 2.5000,  # FAKE external value, MUST BE IGNORED
        "change_24h": 99.9,  # FAKE external value, MUST BE IGNORED
        "timestamp": 1700000000.0,
    }

    result = processor.process_raw_message(tick)
    assert result is not None
    assert result.symbol == "EURUSD"
    assert result.buy == 1.1000
    assert result.sell == 1.1010
    assert result.mid == pytest.approx(1.1005)
    # Computed independently: on 1st tick, high=low=mid=1.1005, change_24h=0.0
    assert result.high_24h == pytest.approx(1.1005)
    assert result.low_24h == pytest.approx(1.1005)
    assert result.change_24h == 0.0
    assert result.high_24h != 2.5000
    assert result.low_24h != 0.5000
    assert result.change_24h != 99.9


def test_process_raw_bytes_and_strings():
    """
    Test parsing raw JSON string and raw bytes.
    """
    processor = MarketProcessor()

    # Raw string
    json_str = '{"symbol": "BTCUSD", "buy": 60000.0, "sell": 60010.0, "timestamp": 1700000001}'
    res1 = processor.process_raw_message(json_str)
    assert res1 is not None
    assert res1.symbol == "BTCUSD"
    assert res1.mid == 60005.0

    # Raw bytes
    json_bytes = b'{"symbol": "ETHUSD", "buy": 3000.0, "sell": 3002.0, "timestamp": 1700000002}'
    res2 = processor.process_raw_message(json_bytes)
    assert res2 is not None
    assert res2.symbol == "ETHUSD"
    assert res2.mid == 3001.0


def test_malformed_json_handling():
    """
    Ensure invalid JSON does not raise an unhandled exception or crash the engine.
    """
    processor = MarketProcessor()
    bad_payloads = [
        "not a valid json {",
        b"{'invalid': single_quotes}",
        "",
        {"no_symbol": True, "buy": 1.0, "sell": 1.1},
        {"symbol": "EURUSD", "buy": "not_a_number", "sell": 1.1},
        {"symbol": "EURUSD", "buy": -1.0, "sell": 1.1},  # negative price
        [],
    ]

    for bad in bad_payloads:
        res = processor.process_raw_message(bad)
        assert res is None, f"Expected None for malformed payload: {bad}"


def test_snapshot_generation():
    """
    Test that get_snapshot returns all active symbols or requested subset.
    """
    processor = MarketProcessor()
    processor.process_raw_message({"symbol": "EURUSD", "buy": 1.08, "sell": 1.09, "timestamp": 100})
    processor.process_raw_message({"symbol": "GBPUSD", "buy": 1.25, "sell": 1.26, "timestamp": 100})

    snap_all = processor.get_snapshot()
    assert len(snap_all) == 2
    assert "EURUSD" in snap_all
    assert "GBPUSD" in snap_all

    snap_subset = processor.get_snapshot(["EURUSD"])
    assert len(snap_subset) == 1
    assert "EURUSD" in snap_subset
    assert "GBPUSD" not in snap_subset
