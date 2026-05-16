"""
performance_monitor.py — Latency tracking, context caching, and event indexing.

- PerformanceMonitor: p95 latency per operation
- ContextCache: LRU cache for last 10 reconstructed contexts
- EventIndex: fast service+time window lookup
"""

from __future__ import annotations

import time
from collections import OrderedDict, defaultdict
from typing import Dict, List, Optional, Tuple

from engine.schema import Context

# ---------------------------------------------------------------------------
# Latency Tracker
# ---------------------------------------------------------------------------


class PerformanceMonitor:
    """
    Tracks latency per named operation.
    Computes p50, p95, p99 on a rolling window of the last 1000 measurements.
    """

    def __init__(self, window: int = 1000):
        self._window = window
        # op_name → sorted list of durations (ms)
        self._samples: Dict[str, List[float]] = defaultdict(list)

    def track_latency(self, operation: str, duration_ms: float):
        samples = self._samples[operation]
        samples.append(duration_ms)
        # Keep sorted for percentile computation
        samples.sort()
        if len(samples) > self._window:
            self._samples[operation] = samples[-self._window :]

    def get_percentile(self, operation: str, p: float) -> float:
        """Return p-th percentile latency in ms. p in [0, 100]."""
        samples = self._samples.get(operation, [])
        if not samples:
            return 0.0
        idx = int(len(samples) * p / 100)
        idx = min(idx, len(samples) - 1)
        return samples[idx]

    def get_p95_latency(self, operation: str) -> float:
        return self.get_percentile(operation, 95)

    def get_p50_latency(self, operation: str) -> float:
        return self.get_percentile(operation, 50)

    def summary(self) -> Dict[str, Dict[str, float]]:
        return {
            op: {
                "p50_ms": round(self.get_p50_latency(op), 2),
                "p95_ms": round(self.get_p95_latency(op), 2),
                "count": len(samples),
            }
            for op, samples in self._samples.items()
        }


# ---------------------------------------------------------------------------
# Context Cache — LRU
# ---------------------------------------------------------------------------


class ContextCache:
    """LRU cache for reconstructed contexts. Max 10 entries."""

    def __init__(self, max_size: int = 10):
        self._max = max_size
        self._cache: OrderedDict = OrderedDict()

    def _key(self, incident_id: str, mode: str) -> str:
        return f"{incident_id}::{mode}"

    def get(self, incident_id: str, mode: str) -> Optional[Context]:
        k = self._key(incident_id, mode)
        if k not in self._cache:
            return None
        # Move to end (most recently used)
        self._cache.move_to_end(k)
        return self._cache[k]

    def set(self, incident_id: str, mode: str, context: Context):
        k = self._key(incident_id, mode)
        if k in self._cache:
            self._cache.move_to_end(k)
        self._cache[k] = context
        if len(self._cache) > self._max:
            self._cache.popitem(last=False)  # Evict LRU

    def invalidate(self, incident_id: str):
        for mode in ("fast", "deep"):
            k = self._key(incident_id, mode)
            self._cache.pop(k, None)

    @property
    def size(self) -> int:
        return len(self._cache)


# ---------------------------------------------------------------------------
# Event Index — fast service+time range lookup
# ---------------------------------------------------------------------------


class EventIndex:
    """
    Secondary index over the telemetry buffer.
    Maps (service_name → sorted list of (ts_float, global_idx)).
    Allows O(log N) range queries.
    """

    def __init__(self):
        # service → list of (ts_float, global_idx), kept sorted by ts
        self._index: Dict[str, List[Tuple[float, int]]] = defaultdict(list)
        self._last_indexed = 0

    def add(self, service: str, ts_float: float, global_idx: int):
        """Index one event."""
        self._index[service].append((ts_float, global_idx))

    def query_by_service_time(
        self,
        service: str,
        start_ts: float,
        end_ts: float,
    ) -> List[int]:
        """
        Return global indices of events for service in [start_ts, end_ts].
        Binary search for start; linear scan to end.
        """
        entries = self._index.get(service, [])
        if not entries:
            return []

        # Binary search for start
        lo, hi = 0, len(entries)
        while lo < hi:
            mid = (lo + hi) // 2
            if entries[mid][0] < start_ts:
                lo = mid + 1
            else:
                hi = mid

        results = []
        for i in range(lo, len(entries)):
            ts_f, idx = entries[i]
            if ts_f > end_ts:
                break
            results.append(idx)
        return results

    def services(self) -> List[str]:
        return list(self._index.keys())


# ---------------------------------------------------------------------------
# Timer context manager
# ---------------------------------------------------------------------------


class Timer:
    """Context manager to measure execution time."""

    def __init__(self):
        self.elapsed_ms: float = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000
