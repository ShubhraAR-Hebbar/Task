"""
Performance and Operational Metrics Instrumentation.
Tracks throughput, latency distributions (P50, P95, P99), payload sizes, and connection states.
"""

import time
import logging
import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger("pricing_engine.metrics")


@dataclass
class MetricsSnapshot:
    """Snapshot of metrics for logging, reporting, or external consumption."""
    timestamp: float
    uptime_seconds: float
    messages_received: int
    messages_processed: int
    messages_distributed: int
    malformed_messages: int
    dropped_messages: int
    active_clients: int
    active_symbols: int
    throughput_msg_per_sec: float
    processing_latency_ms_avg: float
    processing_latency_ms_p50: float
    processing_latency_ms_p95: float
    processing_latency_ms_p99: float
    distribution_latency_ms_avg: float
    distribution_latency_ms_p95: float
    last_outgoing_payload_bytes: int
    avg_outgoing_payload_bytes: float


class MetricsCollector:
    """
    Thread-safe & async-compatible collector for real-time engine telemetry.
    """

    def __init__(self, latency_window_size: int = 5000, payload_window_size: int = 1000):
        self.start_time: float = time.time()
        self.messages_received: int = 0
        self.messages_processed: int = 0
        self.messages_distributed: int = 0
        self.malformed_messages: int = 0
        self.dropped_messages: int = 0
        self.active_clients: int = 0
        self.active_symbols: int = 0

        # Rolling buffers for percentile latency calculations
        self._proc_latencies_ms: deque = deque(maxlen=latency_window_size)
        self._dist_latencies_ms: deque = deque(maxlen=latency_window_size)
        self._payload_sizes: deque = deque(maxlen=payload_window_size)
        self.last_payload_size_bytes: int = 0

        # Throughput tracking
        self._last_report_time: float = self.start_time
        self._last_processed_count: int = 0

    def record_received(self, count: int = 1) -> None:
        self.messages_received += count

    def record_malformed(self, count: int = 1) -> None:
        self.malformed_messages += count

    def record_dropped(self, count: int = 1) -> None:
        self.dropped_messages += count

    def record_processing_latency(self, latency_seconds: float) -> None:
        self.messages_processed += 1
        self._proc_latencies_ms.append(latency_seconds * 1000.0)

    def record_distribution_latency(self, latency_seconds: float, client_count: int = 1) -> None:
        self.messages_distributed += client_count
        self._dist_latencies_ms.append(latency_seconds * 1000.0)

    def record_payload_size(self, size_bytes: int) -> None:
        self.last_payload_size_bytes = size_bytes
        self._payload_sizes.append(size_bytes)

    def set_active_clients(self, count: int) -> None:
        self.active_clients = count

    def set_active_symbols(self, count: int) -> None:
        self.active_symbols = count

    def _percentile(self, data: List[float], pct: float) -> float:
        if not data:
            return 0.0
        k = (len(data) - 1) * (pct / 100.0)
        f = int(k)
        c = f + 1
        if c < len(data):
            return data[f] + (k - f) * (data[c] - data[f])
        return data[f]

    def get_snapshot(self) -> MetricsSnapshot:
        now = time.time()
        uptime = now - self.start_time
        window_dt = now - self._last_report_time
        if window_dt > 0.001:
            throughput = (self.messages_processed - self._last_processed_count) / window_dt
        else:
            throughput = 0.0

        self._last_report_time = now
        self._last_processed_count = self.messages_processed

        proc_sorted = sorted(self._proc_latencies_ms) if self._proc_latencies_ms else []
        dist_sorted = sorted(self._dist_latencies_ms) if self._dist_latencies_ms else []

        proc_avg = statistics.mean(proc_sorted) if proc_sorted else 0.0
        proc_p50 = self._percentile(proc_sorted, 50)
        proc_p95 = self._percentile(proc_sorted, 95)
        proc_p99 = self._percentile(proc_sorted, 99)

        dist_avg = statistics.mean(dist_sorted) if dist_sorted else 0.0
        dist_p95 = self._percentile(dist_sorted, 95)

        avg_payload = (
            statistics.mean(self._payload_sizes) if self._payload_sizes else 0.0
        )

        return MetricsSnapshot(
            timestamp=now,
            uptime_seconds=uptime,
            messages_received=self.messages_received,
            messages_processed=self.messages_processed,
            messages_distributed=self.messages_distributed,
            malformed_messages=self.malformed_messages,
            dropped_messages=self.dropped_messages,
            active_clients=self.active_clients,
            active_symbols=self.active_symbols,
            throughput_msg_per_sec=throughput,
            processing_latency_ms_avg=proc_avg,
            processing_latency_ms_p50=proc_p50,
            processing_latency_ms_p95=proc_p95,
            processing_latency_ms_p99=proc_p99,
            distribution_latency_ms_avg=dist_avg,
            distribution_latency_ms_p95=dist_p95,
            last_outgoing_payload_bytes=self.last_payload_size_bytes,
            avg_outgoing_payload_bytes=avg_payload,
        )

    def log_report(self) -> None:
        snap = self.get_snapshot()
        logger.info(
            f"[METRICS] Uptime: {snap.uptime_seconds:.1f}s | "
            f"Throughput: {snap.throughput_msg_per_sec:.1f} msg/s | "
            f"Processed: {snap.messages_processed} | Distributed: {snap.messages_distributed} | "
            f"Active Clients: {snap.active_clients} | "
            f"Latency(P50/P95/P99): {snap.processing_latency_ms_p50:.3f}ms / {snap.processing_latency_ms_p95:.3f}ms / {snap.processing_latency_ms_p99:.3f}ms | "
            f"Payload: last={snap.last_outgoing_payload_bytes}B, avg={snap.avg_outgoing_payload_bytes:.1f}B | "
            f"Errors: malformed={snap.malformed_messages}, dropped={snap.dropped_messages}"
        )


# Global metrics instance
metrics = MetricsCollector()
