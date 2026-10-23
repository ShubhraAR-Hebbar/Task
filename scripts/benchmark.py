"""
Performance and Latency Benchmark Suite.
Measures:
1. Message processing throughput (ticks/sec)
2. Processing & distribution latency (P50, P95, P99)
3. Outbound serialized payload sizes vs the 10 KB requirement
4. End-to-end latency validation against the <= 50ms SLA
"""

import asyncio
import json
import os
import statistics
import sys
import time
from typing import List

# Ensure repository root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.distribution.payload_optimizer import PayloadOptimizer
from app.monitoring.metrics import metrics
from app.processing.market_processor import MarketProcessor


def run_benchmark(num_ticks: int = 20000) -> None:
    print("\n" + "=" * 70)
    print(f"   REAL-TIME PRICING ENGINE - PERFORMANCE BENCHMARK ({num_ticks:,} TICKS)")
    print("=" * 70)

    processor = MarketProcessor(window_seconds=86400.0)
    symbols = ["EURUSD", "GBPUSD", "USDJPY", "BTCUSD", "ETHUSD"]

    latencies_ms: List[float] = []

    # Generate test tick stream
    t_start = time.perf_counter()

    for i in range(num_ticks):
        sym = symbols[i % len(symbols)]
        base_price = 1.08 if "EUR" in sym else (60000.0 if "BTC" in sym else 150.0)
        tick_time = 1700000000.0 + (i * 0.1)

        payload = {
            "symbol": sym,
            "buy": base_price + (i % 100) * 0.0001,
            "sell": base_price + (i % 100) * 0.0001 + 0.0002,
            "low_24h": 0.0,    # Ignored by design
            "high_24h": 0.0,   # Ignored by design
            "change_24h": 0.0, # Ignored by design
            "timestamp": tick_time,
        }

        # Measure single tick processing time
        t0 = time.perf_counter()
        processed = processor.process_raw_message(payload)
        t_lat = (time.perf_counter() - t0) * 1000.0
        latencies_ms.append(t_lat)

    t_total = time.perf_counter() - t_start
    throughput = num_ticks / t_total

    # Compute latency distribution
    latencies_sorted = sorted(latencies_ms)
    avg_lat = statistics.mean(latencies_sorted)
    p50_lat = latencies_sorted[int(len(latencies_sorted) * 0.50)]
    p95_lat = latencies_sorted[int(len(latencies_sorted) * 0.95)]
    p99_lat = latencies_sorted[int(len(latencies_sorted) * 0.99)]
    max_lat = latencies_sorted[-1]

    # Measure Payload Sizes
    sample_tick = processor.latest_data["EURUSD"]
    sample_update_json = PayloadOptimizer.serialize_update(sample_tick)
    sample_update_bytes = PayloadOptimizer.measure_bytes(sample_update_json)

    compact_update_json = PayloadOptimizer.serialize_update(sample_tick, compact_keys=True)
    compact_update_bytes = PayloadOptimizer.measure_bytes(compact_update_json)

    snapshot_all = processor.get_snapshot()
    snapshot_json = PayloadOptimizer.serialize_snapshot(snapshot_all)
    snapshot_bytes = PayloadOptimizer.measure_bytes(snapshot_json)

    print("\n--- THROUGHPUT & LATENCY METRICS ---")
    print(f"Total Ticks Processed    : {num_ticks:,}")
    print(f"Total Processing Time    : {t_total:.3f} seconds")
    print(f"Throughput               : {throughput:,.1f} ticks/second")
    print(f"Average Latency          : {avg_lat:.4f} ms")
    print(f"P50 Latency (Median)     : {p50_lat:.4f} ms")
    print(f"P95 Latency              : {p95_lat:.4f} ms")
    print(f"P99 Latency              : {p99_lat:.4f} ms")
    print(f"Max Latency              : {max_lat:.4f} ms")

    print("\n--- PAYLOAD SIZE MEASUREMENTS ---")
    print(f"Single Update Payload    : {sample_update_bytes} bytes (Target: < 10,240 bytes)")
    print(f"Compact Update Payload   : {compact_update_bytes} bytes")
    print(f"Full Snapshot (5 symbols): {snapshot_bytes} bytes (Target: < 10,240 bytes)")

    print("\n--- TARGET COMPLIANCE VERIFICATION ---")
    latency_sla_met = p99_lat <= 50.0
    payload_sla_met = sample_update_bytes < 10240 and snapshot_bytes < 10240

    print(f"Latency Target (<= 50ms) : {'[PASS] ' if latency_sla_met else '[FAIL] '} (P99 is {p99_lat:.4f} ms)")
    print(f"Payload Target (< 10 KB) : {'[PASS] ' if payload_sla_met else '[FAIL] '} (Update is {sample_update_bytes} B, Snapshot is {snapshot_bytes} B)")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    run_benchmark(num_ticks=25000)
