"""
Unit and integration tests for Redis state persistence and crash recovery.
Uses fakeredis for fast, self-contained, and deterministic async tests.
"""

import time
import pytest
import fakeredis.aioredis
from app.processing.market_processor import MarketProcessor, ProcessedMarketData
from app.storage.redis_store import RedisStore
from app.storage.recovery import StateRecoveryManager


@pytest.mark.asyncio
async def test_redis_persistence_and_recovery():
    """
    Test that observations written to Redis can be recovered to restore
    an exact identical rolling 24-hour window state after a simulated crash.
    """
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)

    store = RedisStore(enabled=True)
    await store.initialize(custom_client=fake_redis)

    # 1. Simulate ticks coming in over time
    now = time.time()
    t1 = now - 3600  # 1 hour ago
    t2 = now - 1800  # 30 mins ago
    t3 = now         # just now

    # Symbol EURUSD:
    # t1: mid=1.1000
    # t2: mid=1.1200 (high)
    # t3: mid=1.0900 (low)
    processor = MarketProcessor(window_seconds=86400.0)
    p1 = processor.process_raw_message({"symbol": "EURUSD", "buy": 1.1000, "sell": 1.1000, "timestamp": t1})
    p2 = processor.process_raw_message({"symbol": "EURUSD", "buy": 1.1200, "sell": 1.1200, "timestamp": t2})
    p3 = processor.process_raw_message({"symbol": "EURUSD", "buy": 1.0900, "sell": 1.0900, "timestamp": t3})

    assert p3.high_24h == pytest.approx(1.1200)
    assert p3.low_24h == pytest.approx(1.0900)
    # Ref is 1.1000, current is 1.0900 -> ((1.09 - 1.10) / 1.10) * 100 = -0.9091%
    expected_change = ((1.0900 - 1.1000) / 1.1000) * 100.0
    assert p3.change_24h == pytest.approx(expected_change, rel=1e-3)

    # Directly persist all three ticks to Redis
    await store._persist_to_redis(p1)
    await store._persist_to_redis(p2)
    await store._persist_to_redis(p3)

    # 2. Simulate complete application crash and restart with a fresh empty MarketProcessor!
    new_processor = MarketProcessor(window_seconds=86400.0)
    assert "EURUSD" not in new_processor.windows

    recovery_mgr = StateRecoveryManager(
        processor=new_processor, redis_store=store, window_seconds=86400.0
    )
    restored_count = await recovery_mgr.restore_state(["EURUSD"])

    assert restored_count == 1
    assert "EURUSD" in new_processor.windows
    recovered_data = new_processor.latest_data["EURUSD"]

    # Verify recovered statistical state matches the pre-crash engine state!
    assert recovered_data.buy == 1.0900
    assert recovered_data.sell == 1.0900
    assert recovered_data.mid == pytest.approx(1.0900)
    assert recovered_data.high_24h == pytest.approx(1.1200)
    assert recovered_data.low_24h == pytest.approx(1.0900)
    assert recovered_data.change_24h == pytest.approx(expected_change, rel=1e-3)
    assert recovered_data.observation_count == 3

    await store.close()


@pytest.mark.asyncio
async def test_recovery_with_corrupted_redis_entries():
    """
    Test that corrupted JSON strings in Redis sorted set are safely skipped
    without failing recovery or crashing the engine.
    """
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = RedisStore(enabled=True)
    await store.initialize(custom_client=fake_redis)

    now = time.time()
    # Inject valid entry
    await fake_redis.zadd(
        "engine:history:BTCUSD",
        {'{"t": ' + str(now - 100) + ', "m": 50000.0, "b": 50000.0, "s": 50000.0}': now - 100},
    )
    # Inject corrupted entry (invalid JSON syntax)
    await fake_redis.zadd("engine:history:BTCUSD", {"INVALID_JSON_{{": now - 50})
    # Inject second valid entry
    await fake_redis.zadd(
        "engine:history:BTCUSD",
        {'{"t": ' + str(now) + ', "m": 55000.0, "b": 55000.0, "s": 55000.0}': now},
    )
    await fake_redis.sadd("engine:symbols", "BTCUSD")

    new_processor = MarketProcessor()
    recovery_mgr = StateRecoveryManager(processor=new_processor, redis_store=store)
    restored = await recovery_mgr.restore_state(["BTCUSD"])

    assert restored == 1
    assert "BTCUSD" in new_processor.windows
    assert new_processor.latest_data["BTCUSD"].observation_count == 2
    assert new_processor.latest_data["BTCUSD"].high_24h == 55000.0

    await store.close()


@pytest.mark.asyncio
async def test_redis_disabled_graceful_handling():
    """
    Test that when Redis is disabled, store initialization and recovery
    succeed gracefully in memory without exceptions.
    """
    store = RedisStore(enabled=False)
    initialized = await store.initialize()
    assert initialized is False
    assert store.is_connected is False

    processor = MarketProcessor()
    recovery_mgr = StateRecoveryManager(processor=processor, redis_store=store)
    restored = await recovery_mgr.restore_state(["EURUSD"])
    assert restored == 0

    await store.close()
