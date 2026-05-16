"""
telemetry_buffer.py — Circular event buffer with provenance preservation,
stream validation, and ingestion rate monitoring.
"""

from __future__ import annotations

import bisect
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from engine.schema import Event, EventKind

# ---------------------------------------------------------------------------
# Required fields per event kind
# ---------------------------------------------------------------------------
_REQUIRED: Dict[str, List[str]] = {
    EventKind.DEPLOY: ["ts", "kind", "service", "version"],
    EventKind.METRIC: ["ts", "kind", "service", "name", "value"],
    EventKind.LOG: ["ts", "kind", "service", "level", "msg"],
    EventKind.TRACE: ["ts", "kind", "trace_id", "spans"],
    EventKind.TOPOLOGY: ["ts", "kind", "change"],
    EventKind.INCIDENT_SIGNAL: ["ts", "kind", "incident_id", "trigger"],
    EventKind.REMEDIATION: ["ts", "kind", "incident_id", "action", "target", "outcome"],
}

_VALID_KINDS = set(k.value for k in EventKind)


def _parse_ts(ts_str: str) -> float:
    """Return UTC timestamp as float seconds since epoch."""
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return 0.0


class TelemetryBuffer:
    """
    Append-only circular buffer for telemetry events.

    - Preserves full JSON provenance (original dict + _index + _ingested_at)
    - Maintains secondary indices: by service, by kind, by timestamp
    - Tracks ingestion rate (events/sec, rolling 10s window)
    - Thread-safe for single-writer multi-reader (GIL is sufficient here)
    """

    def __init__(self, max_events: int = 1_000_000):
        self._max = max_events
        # Tiered Retention: Separate buffers for high vs low signal
        self._priority_buffer: List[Optional[Event]] = [None] * (max_events // 2)
        self._standard_buffer: List[Optional[Event]] = [None] * (max_events // 2)
        self._p_max = max_events // 2
        self._s_max = max_events // 2
        
        self._p_head = 0
        self._s_head = 0
        self._p_count = 0
        self._s_count = 0

        # Semantic Compression: Track the last log seen for each service
        # (service, level, msg_hash) -> EventIndex
        self._last_log_state: Dict[Tuple[str, str, int], int] = {}
        
        # Secondary indices (shared across buffers)
        self._service_idx: Dict[str, List[int]] = defaultdict(list)
        self._kind_idx: Dict[str, List[int]] = defaultdict(list)
        self._ts_list: List[Tuple[float, int]] = []
        self._service_ts_idx: Dict[str, List[Tuple[float, int]]] = defaultdict(list)
        self._kind_ts_idx: Dict[str, List[Tuple[float, int]]] = defaultdict(list)

        self._event_freq: Dict[str, int] = defaultdict(int)
        self._rate_window: deque = deque()
        self._rate_window_sec = 10.0
        self._total_ingested = 0
        self._rejected = 0
        self._accepted = 0

    # ------------------------------------------------------------------
    # Core write path
    # ------------------------------------------------------------------

    def _is_priority(self, event: Event) -> bool:
        """Determine if an event belongs in the high-retention pool."""
        kind = event.get("kind")
        if kind in (EventKind.DEPLOY.value, EventKind.INCIDENT_SIGNAL.value, 
                    EventKind.TOPOLOGY.value, EventKind.REMEDIATION.value):
            return True
        if kind == EventKind.LOG.value and event.get("level", "").lower() in ("error", "warn", "fatal"):
            return True
        return False

    PRIORITY_OFFSET = 10**12

    def append(self, event: Event) -> int:
        """
        Validate and append one event with priority routing and semantic compression.
        Returns the global event index. Returns -1 if rejected.
        """
        kind = event.get("kind", "")
        if kind not in _VALID_KINDS:
            self._rejected += 1
            return -1

        # 1. Semantic Compression for repetitive INFO logs
        if kind == EventKind.LOG.value and event.get("level", "").lower() == "info":
            svc = event.get("service", "")
            msg = event.get("msg", "")
            msg_hash = hash(msg)
            comp_key = (svc, "info", msg_hash)
            
            if comp_key in self._last_log_state:
                prev_idx = self._last_log_state[comp_key]
                prev_ev = self.get_by_index(prev_idx)
                if prev_ev:
                    # Collapse: update count and timestamp instead of appending
                    prev_ev["_count"] = prev_ev.get("_count", 1) + 1
                    prev_ev["ts_end"] = event.get("ts")
                    return prev_idx

        # Soft validation
        required = _REQUIRED.get(kind, [])
        for f in required:
            if f not in event:
                break

        # 2. Priority Routing
        is_p = self._is_priority(event)
        stamped = dict(event)
        
        if is_p:
            global_idx = self.PRIORITY_OFFSET + self._p_count
            buf_pos = self._p_head
            self._priority_buffer[buf_pos] = stamped
            self._p_head = (self._p_head + 1) % self._p_max
            self._p_count += 1
        else:
            global_idx = self._s_count
            buf_pos = self._s_head
            self._standard_buffer[buf_pos] = stamped
            self._s_head = (self._s_head + 1) % self._s_max
            self._s_count += 1

        stamped["_index"] = global_idx
        stamped["_ingested_at"] = time.time()
        
        # Track for compression next time
        if kind == EventKind.LOG.value and event.get("level", "").lower() == "info":
            self._last_log_state[(event.get("service"), "info", hash(event.get("msg")))] = global_idx

        # 3. Indexing
        ts_f = _parse_ts(event.get("ts", ""))
        ts_entry = (ts_f, global_idx)
        self._ts_list.append(ts_entry)
        
        svc = event.get("service") or event.get("target") or ""
        if svc:
            self._service_ts_idx[svc].append(ts_entry)
        self._kind_ts_idx[kind].append(ts_entry)

        # TF-IDF tracking
        freq_key = f"{kind}:{event.get('name', '')}" if kind == EventKind.METRIC.value else kind
        if kind == EventKind.LOG.value:
            freq_key = f"{kind}:{event.get('level', '').lower()}"
        self._event_freq[freq_key] += 1

        # Rate tracking
        now = time.time()
        self._rate_window.append(now)
        while self._rate_window and now - self._rate_window[0] > self._rate_window_sec:
            self._rate_window.popleft()

        self._accepted += 1
        return global_idx


    def ingest_many(self, events: Iterable[Event]) -> List[int]:
        """Batch ingest. Returns list of assigned global indices."""
        return [self.append(e) for e in events]

    # ------------------------------------------------------------------
    # Read paths
    # ------------------------------------------------------------------

    def get_by_index(self, global_idx: int) -> Optional[Event]:
        """Retrieve event by its global index from the appropriate pool."""
        if global_idx < 0:
            return None
        
        if global_idx >= self.PRIORITY_OFFSET:
            # Priority Pool
            p_idx = global_idx - self.PRIORITY_OFFSET
            if p_idx >= self._p_count:
                return None
            if self._p_count > self._p_max:
                oldest = self._p_count - self._p_max
                if p_idx < oldest:
                    return None # Evicted
            return self._priority_buffer[p_idx % self._p_max]
        else:
            # Standard Pool
            if global_idx >= self._s_count:
                return None
            if self._s_count > self._s_max:
                oldest = self._s_count - self._s_max
                if global_idx < oldest:
                    return None # Evicted
            return self._standard_buffer[global_idx % self._s_max]

    def get_range_by_ts(self, start_ts: float, end_ts: float) -> List[Event]:
        """Return all events whose ts falls in [start_ts, end_ts]."""
        return self._filter_by_ts(self._ts_list, start_ts, end_ts)

    def _filter_by_ts(
        self, ts_list: List[Tuple[float, int]], start_ts: float, end_ts: float
    ) -> List[Event]:
        """Helper to binary search a (ts, idx) list and return events."""
        if not ts_list:
            return []

        # Find start index
        start_idx = bisect.bisect_left(ts_list, (start_ts, -1))
        # Find end index
        end_idx = bisect.bisect_right(ts_list, (end_ts, float("inf")))

        results = []
        for i in range(start_idx, end_idx):
            ev = self.get_by_index(ts_list[i][1])
            if ev is not None:
                results.append(ev)
        return results

    def get_by_service(
        self,
        service: str,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
    ) -> List[Event]:
        """Return events for a service, optionally filtered by time range."""
        ts_list = self._service_ts_idx.get(service, [])
        if start_ts is None and end_ts is None:
            # Return all
            return [
                self.get_by_index(idx)
                for _ts, idx in ts_list
                if self.get_by_index(idx) is not None
            ]

        return self._filter_by_ts(ts_list, start_ts or -1.0, end_ts or float("inf"))

    def get_by_services(
        self,
        services: Iterable[str],
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
    ) -> List[Event]:
        """Union query across multiple service names."""
        seen = set()
        results = []
        for svc in services:
            for ev in self.get_by_service(svc, start_ts, end_ts):
                idx = ev.get("_index")
                if idx not in seen:
                    seen.add(idx)
                    results.append(ev)
        return sorted(results, key=lambda e: e.get("_index", 0))

    def get_recent(self, n: int) -> List[Event]:
        """Return the n most recently appended events."""
        results = []
        count = min(n, self._size)
        for i in range(count):
            pos = (self._head - 1 - i) % self._max
            ev = self._buffer[pos]
            if ev is not None:
                results.append(ev)
        return list(reversed(results))

    def get_by_kind(
        self,
        kind: str,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
    ) -> List[Event]:
        ts_list = self._kind_ts_idx.get(kind, [])
        if start_ts is None and end_ts is None:
            return [
                self.get_by_index(idx)
                for _ts, idx in ts_list
                if self.get_by_index(idx) is not None
            ]

        return self._filter_by_ts(ts_list, start_ts or -1.0, end_ts or float("inf"))

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_ingestion_rate(self) -> float:
        """Events per second over the rolling window."""
        if not self._rate_window:
            return 0.0
        now = time.time()
        while self._rate_window and now - self._rate_window[0] > self._rate_window_sec:
            self._rate_window.popleft()
        return len(self._rate_window) / self._rate_window_sec

    @property
    def _count(self) -> int:
        return self._p_count + self._s_count

    @property
    def _size(self) -> int:
        return min(self._p_count, self._p_max) + min(self._s_count, self._s_max)

    @property
    def total_count(self) -> int:
        return self._count

    @property
    def valid_size(self) -> int:
        return self._size

    def export_json_summary(self) -> Dict:
        return {
            "total_ingested": self._total_ingested,
            "accepted": self._accepted,
            "rejected": self._rejected,
            "current_size": self._size,
            "ingestion_rate_eps": round(self.get_ingestion_rate(), 2),
            "services_tracked": list(self._service_idx.keys()),
        }

    def get_idf_weight(self, event: Event) -> float:
        """Inverse Document Frequency weighting for Rare-Event Amplification."""
        import math

        kind = event.get("kind", "")
        freq_key = (
            f"{kind}:{event.get('name', '')}"
            if kind == EventKind.METRIC.value
            else kind
        )
        if kind == EventKind.LOG.value:
            freq_key = f"{kind}:{event.get('level', '').lower()}"

        count = self._event_freq.get(freq_key, 1)
        total = max(1, self._count)
        idf = math.log10((total + 1) / (count + 1))

        max_idf = math.log10(total + 1)
        if max_idf <= 0:
            return 0.5
        # Range roughly [0.1, 1.0] where 1.0 is rare and 0.1 is common
        return min(1.0, max(0.1, idf / max_idf))
