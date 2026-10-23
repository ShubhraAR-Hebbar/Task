"""
High-performance rolling 24-hour market statistics window.
Uses dual monotonic deques for O(1) amortized rolling High and Low calculations.
"""

from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple


@dataclass(slots=True)
class WindowStats:
    """Calculated market statistics for a symbol."""
    buy: float
    sell: float
    mid: float
    high_24h: float
    low_24h: float
    change_24h: float
    timestamp: float
    observation_count: int


@dataclass(slots=True)
class Observation:
    """Single timestamped price point."""
    timestamp: float
    mid: float
    buy: float
    sell: float


class RollingWindow:
    """
    Maintains a rolling time window (default 24h = 86,400s) for a single symbol.

    Formulas and Price Basis:
    - Consistent Price Basis: Mid-Price P_mid = (buy + sell) / 2.0.
      Mid-price represents the unbiased fair market clearing level, preventing
      spread widening from creating artificial price peaks or troughs.
    - Rolling 24h High: Max(P_mid) for all observations where t >= T_now - 86,400s.
    - Rolling 24h Low: Min(P_mid) for all observations where t >= T_now - 86,400s.
    - Rolling 24h Percentage Change:
        change_24h = ((P_latest - P_ref) / P_ref) * 100.0
      where P_ref is the earliest mid-price observation in the current rolling window.
      Treatment of Missing Historical Observations:
      If only a single observation exists (e.g. system cold start or newly listed symbol),
      no external reference is fabricated; change_24h is evaluated as 0.0.
      As ticks accumulate, the window anchors to the earliest available tick until
      24 hours of continuous history have elapsed, after which P_ref precisely tracks
      the observation at T_latest - 24 hours.

    Complexity:
    - Amortized O(1) per tick for addition, expiration, min, max, and % change.
    """

    def __init__(self, window_seconds: float = 86400.0):
        self.window_seconds: float = window_seconds
        self.observations: Deque[Observation] = deque()
        # Decreasing monotonic deque: stores (timestamp, mid_price) with descending prices
        self.max_deque: Deque[Tuple[float, float]] = deque()
        # Increasing monotonic deque: stores (timestamp, mid_price) with ascending prices
        self.min_deque: Deque[Tuple[float, float]] = deque()

        # Cache of latest calculated stats
        self.latest_stats: Optional[WindowStats] = None

    def add_observation(
        self,
        timestamp: float,
        buy: float,
        sell: float,
        mid: Optional[float] = None,
    ) -> WindowStats:
        """
        Ingests a new price tick and returns updated 24-hour window statistics.
        Handles sequential ticks in O(1), duplicates, and out-of-order updates safely.
        """
        if mid is None:
            mid = (buy + sell) / 2.0

        new_obs = Observation(timestamp=timestamp, mid=mid, buy=buy, sell=sell)

        # Handle empty window
        if not self.observations:
            self.observations.append(new_obs)
            self.max_deque.append((timestamp, mid))
            self.min_deque.append((timestamp, mid))
            self.latest_stats = WindowStats(
                buy=buy,
                sell=sell,
                mid=mid,
                high_24h=mid,
                low_24h=mid,
                change_24h=0.0,
                timestamp=timestamp,
                observation_count=1,
            )
            return self.latest_stats

        last_obs = self.observations[-1]

        # 1. Check for Duplicate (same timestamp and prices)
        if timestamp == last_obs.timestamp and buy == last_obs.buy and sell == last_obs.sell:
            # Idempotent skip: return current stats
            return self.latest_stats

        # 2. Check for Out-of-order updates (timestamp is older than last observation)
        if timestamp < last_obs.timestamp:
            return self._handle_out_of_order(new_obs)

        # 3. Normal Sequential Ingestion (timestamp >= last_obs.timestamp)
        # Update Decreasing Monotonic Deque for Rolling Maximum
        while self.max_deque and self.max_deque[-1][1] <= mid:
            self.max_deque.pop()
        self.max_deque.append((timestamp, mid))

        # Update Increasing Monotonic Deque for Rolling Minimum
        while self.min_deque and self.min_deque[-1][1] >= mid:
            self.min_deque.pop()
        self.min_deque.append((timestamp, mid))

        self.observations.append(new_obs)

        # 4. Expire observations older than (timestamp - window_seconds)
        self._expire_old(timestamp)

        # 5. Calculate Rolling Statistics
        high_24h = self.max_deque[0][1]
        low_24h = self.min_deque[0][1]

        # Reference price for percentage change
        if len(self.observations) > 1:
            p_ref = self.observations[0].mid
            if p_ref > 0:
                change_24h = ((mid - p_ref) / p_ref) * 100.0
            else:
                change_24h = 0.0
        else:
            change_24h = 0.0

        self.latest_stats = WindowStats(
            buy=buy,
            sell=sell,
            mid=mid,
            high_24h=high_24h,
            low_24h=low_24h,
            change_24h=round(change_24h, 4),
            timestamp=timestamp,
            observation_count=len(self.observations),
        )
        return self.latest_stats

    def _expire_old(self, current_time: float) -> None:
        """
        Removes observations older than current_time - window_seconds.
        Maintains consistency of monotonic deques.
        """
        cutoff = current_time - self.window_seconds
        while self.observations and self.observations[0].timestamp < cutoff:
            self.observations.popleft()

        while self.max_deque and self.max_deque[0][0] < cutoff:
            self.max_deque.popleft()

        while self.min_deque and self.min_deque[0][0] < cutoff:
            self.min_deque.popleft()

    def _handle_out_of_order(self, new_obs: Observation) -> WindowStats:
        """
        Inserts an out-of-order observation into chronological position
        and reconstructs monotonic deques if within active window.
        """
        latest_ts = self.observations[-1].timestamp
        cutoff = latest_ts - self.window_seconds

        # Reject if already expired past the 24-hour cutoff
        if new_obs.timestamp < cutoff:
            # Observation is too stale; return current stats
            return self.latest_stats

        # Check for duplicate observation in current window
        for obs in self.observations:
            if obs.timestamp == new_obs.timestamp and obs.buy == new_obs.buy and obs.sell == new_obs.sell:
                # Idempotent skip for out-of-order duplicate
                return self.latest_stats

        # Insert in sorted order
        # Since out-of-order updates are typically only slight network jitter,
        # iterate from the right side for O(k) insertion
        inserted = False
        temp_list = list(self.observations)
        for i in range(len(temp_list) - 1, -1, -1):
            if temp_list[i].timestamp <= new_obs.timestamp:
                temp_list.insert(i + 1, new_obs)
                inserted = True
                break
        if not inserted:
            temp_list.insert(0, new_obs)

        self.observations = deque(temp_list)
        self.rebuild_deques()
        self._expire_old(self.observations[-1].timestamp)

        # Recalculate latest stats
        latest = self.observations[-1]
        high_24h = self.max_deque[0][1] if self.max_deque else latest.mid
        low_24h = self.min_deque[0][1] if self.min_deque else latest.mid
        p_ref = self.observations[0].mid
        change_24h = ((latest.mid - p_ref) / p_ref * 100.0) if (len(self.observations) > 1 and p_ref > 0) else 0.0

        self.latest_stats = WindowStats(
            buy=latest.buy,
            sell=latest.sell,
            mid=latest.mid,
            high_24h=high_24h,
            low_24h=low_24h,
            change_24h=round(change_24h, 4),
            timestamp=latest.timestamp,
            observation_count=len(self.observations),
        )
        return self.latest_stats

    def rebuild_deques(self) -> None:
        """
        Reconstructs the max and min monotonic deques from current observations.
        Called after out-of-order insertions or historical state restoration.
        """
        self.max_deque.clear()
        self.min_deque.clear()

        for obs in self.observations:
            t = obs.timestamp
            m = obs.mid

            while self.max_deque and self.max_deque[-1][1] <= m:
                self.max_deque.pop()
            self.max_deque.append((t, m))

            while self.min_deque and self.min_deque[-1][1] >= m:
                self.min_deque.pop()
            self.min_deque.append((t, m))

    def load_history(self, history: List[Tuple[float, float, float, float]]) -> None:
        """
        Bulk loads historical observations (timestamp, mid, buy, sell).
        Used during startup recovery from Redis.
        """
        if not history:
            return

        # Sort history by timestamp
        sorted_history = sorted(history, key=lambda x: x[0])
        now_ts = sorted_history[-1][0]
        cutoff = now_ts - self.window_seconds

        self.observations.clear()
        for ts, mid, buy, sell in sorted_history:
            if ts >= cutoff:
                self.observations.append(Observation(timestamp=ts, mid=mid, buy=buy, sell=sell))

        if self.observations:
            self.rebuild_deques()
            latest = self.observations[-1]
            p_ref = self.observations[0].mid
            change_24h = ((latest.mid - p_ref) / p_ref * 100.0) if (len(self.observations) > 1 and p_ref > 0) else 0.0

            self.latest_stats = WindowStats(
                buy=latest.buy,
                sell=latest.sell,
                mid=latest.mid,
                high_24h=self.max_deque[0][1],
                low_24h=self.min_deque[0][1],
                change_24h=round(change_24h, 4),
                timestamp=latest.timestamp,
                observation_count=len(self.observations),
            )
