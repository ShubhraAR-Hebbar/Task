"""
Unit tests for the 24-hour Rolling Window calculation and Monotonic Deque logic.
"""

import time
import pytest
from app.processing.rolling_window import RollingWindow


def test_single_observation_stats():
    """
    Test that on first observation, high=low=mid and change_24h=0.0
    without inventing a false historical reference.
    """
    window = RollingWindow(window_seconds=86400.0)
    stats = window.add_observation(timestamp=1000.0, buy=1.1000, sell=1.1010)

    assert stats.mid == pytest.approx(1.1005)
    assert stats.high_24h == pytest.approx(1.1005)
    assert stats.low_24h == pytest.approx(1.1005)
    assert stats.change_24h == 0.0
    assert stats.observation_count == 1


def test_rolling_high_low_monotonicity():
    """
    Test that monotonic deques accurately track high and low as prices fluctuate.
    """
    window = RollingWindow(window_seconds=86400.0)

    # Sequence of prices
    # t=10: mid=100.0
    # t=20: mid=105.0 (new high)
    # t=30: mid=95.0  (new low)
    # t=40: mid=98.0  (between low and high)
    window.add_observation(timestamp=10.0, buy=100.0, sell=100.0)
    window.add_observation(timestamp=20.0, buy=105.0, sell=105.0)
    window.add_observation(timestamp=30.0, buy=95.0, sell=95.0)
    stats = window.add_observation(timestamp=40.0, buy=98.0, sell=98.0)

    assert stats.high_24h == pytest.approx(105.0)
    assert stats.low_24h == pytest.approx(95.0)
    # Reference price is at t=10 (100.0). Current price is 98.0 -> -2.0%
    assert stats.change_24h == pytest.approx(-2.0)
    assert stats.observation_count == 4


def test_window_expiration():
    """
    Test that observations older than window_seconds are cleanly popped
    and high/low/ref are updated.
    """
    window_duration = 100.0  # 100 seconds window for easy testing
    window = RollingWindow(window_seconds=window_duration)

    # t=0: peak price 200.0
    window.add_observation(timestamp=0.0, buy=200.0, sell=200.0)
    # t=50: price 150.0
    window.add_observation(timestamp=50.0, buy=150.0, sell=150.0)
    # t=80: price 120.0
    stats1 = window.add_observation(timestamp=80.0, buy=120.0, sell=120.0)

    assert stats1.high_24h == pytest.approx(200.0)
    assert stats1.low_24h == pytest.approx(120.0)
    assert stats1.observation_count == 3

    # Now advance time past window_duration: t=110.0 (t=0 has expired since 110 - 100 = 10 > 0)
    stats2 = window.add_observation(timestamp=110.0, buy=130.0, sell=130.0)

    # t=0 (200.0) is expired! The remaining observations are t=50 (150.0), t=80 (120.0), t=110 (130.0)
    assert stats2.high_24h == pytest.approx(150.0)
    assert stats2.low_24h == pytest.approx(120.0)
    assert stats2.observation_count == 3
    # New reference price is at t=50 (150.0). Latest is 130.0 -> ((130 - 150)/150)*100 = -13.3333%
    expected_change = ((130.0 - 150.0) / 150.0) * 100.0
    assert stats2.change_24h == pytest.approx(expected_change, rel=1e-3)


def test_duplicate_tick_handling():
    """
    Test that duplicate updates with identical timestamp and prices are idempotently skipped.
    """
    window = RollingWindow(window_seconds=86400.0)
    window.add_observation(timestamp=100.0, buy=1.0500, sell=1.0510)
    stats = window.add_observation(timestamp=100.0, buy=1.0500, sell=1.0510)

    assert stats.observation_count == 1


def test_out_of_order_tick_insertion():
    """
    Test that ticks arriving slightly out of order are safely sorted into the window
    and monotonic deques are reconstructed.
    """
    window = RollingWindow(window_seconds=86400.0)
    window.add_observation(timestamp=100.0, buy=10.0, sell=10.0)
    window.add_observation(timestamp=120.0, buy=20.0, sell=20.0)

    # Now out-of-order tick arrives with timestamp 110.0 and extreme high price 30.0
    stats = window.add_observation(timestamp=110.0, buy=30.0, sell=30.0)

    # Observation count should be 3
    assert stats.observation_count == 3
    # High should be 30.0 (from the inserted tick)
    assert stats.high_24h == pytest.approx(30.0)
    # Low should be 10.0 (from t=100)
    assert stats.low_24h == pytest.approx(10.0)
    # Latest observation is still t=120 (mid=20.0), ref is t=100 (mid=10.0) -> +100%
    assert stats.change_24h == pytest.approx(100.0)


def test_stale_tick_rejection():
    """
    Test that an out-of-order tick that is already older than the 24h cutoff is rejected.
    """
    window = RollingWindow(window_seconds=100.0)
    window.add_observation(timestamp=200.0, buy=10.0, sell=10.0)

    # Tick at t=50 (older than 200 - 100 = 100)
    stats = window.add_observation(timestamp=50.0, buy=999.0, sell=999.0)

    # Should not contaminate high
    assert stats.high_24h == pytest.approx(10.0)
    assert stats.observation_count == 1


def test_interspersed_out_of_order_and_duplicates():
    """
    Test a series of out-of-order and duplicate ticks interspersed together.
    """
    window = RollingWindow(window_seconds=100.0)
    # Stream in order: t=10(50), t=30(70), t=50(60)
    window.add_observation(10.0, 50.0, 50.0)
    window.add_observation(30.0, 70.0, 70.0)
    window.add_observation(50.0, 60.0, 60.0)

    # Interspersed duplicate of t=30
    window.add_observation(30.0, 70.0, 70.0)
    assert len(window.observations) == 3

    # Out of order: t=20 (80 - new high)
    s1 = window.add_observation(20.0, 80.0, 80.0)
    assert len(window.observations) == 4
    assert s1.high_24h == pytest.approx(80.0)
    assert s1.low_24h == pytest.approx(50.0)

    # Out of order: t=40 (40 - new low)
    s2 = window.add_observation(40.0, 40.0, 40.0)
    assert len(window.observations) == 5
    assert s2.high_24h == pytest.approx(80.0)
    assert s2.low_24h == pytest.approx(40.0)
    # Ref price is still t=10 (50.0). Latest price is t=50 (60.0) -> +20%
    assert s2.change_24h == pytest.approx(20.0)

