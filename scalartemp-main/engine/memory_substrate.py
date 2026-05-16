"""
memory_substrate.py — Long-horizon operational memory.

- ServiceIdentity: stable UUID mapping surviving renames
- BaselineStats: rolling metric stats for anomaly scoring
- IncidentFamilyRegistry: fingerprint storage and fuzzy lookup
"""

from __future__ import annotations

import math
import uuid
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Service Identity — topology-invariant entity tracking
# ---------------------------------------------------------------------------


class ServiceIdentity:
    """
    Maps service names → stable entity IDs (UUIDs).

    When a service is renamed (topology change="rename"),
    the UUID persists so all historical edges remain valid.
    """

    def __init__(self):
        self._name_to_id: Dict[str, str] = {}
        self._id_to_names: Dict[str, List[str]] = defaultdict(
            list
        )  # eid → all names ever
        self._rename_log: List[Tuple[str, str]] = []  # (from_name, to_name)
        self._merged_into: Dict[str, str] = {}  # orphan eid → canonical eid

    def get_or_create(self, service_name: str) -> str:
        """Return existing UUID or create new one for this service name."""
        if service_name not in self._name_to_id:
            eid = str(uuid.uuid4())
            self._name_to_id[service_name] = eid
            self._id_to_names[eid].append(service_name)
        return self._name_to_id[service_name]

    def resolve(self, service_name: str) -> str:
        """
        Return a stable identifier for a service name.
        Uses the internal mapping built from topology events.
        """
        if not service_name:
            return ""
        return self._name_to_id.get(service_name, service_name)

    def on_rename(self, old_name: str, new_name: str) -> str:
        """Update mapping: the new name points to the old entity's UUID."""
        eid = self.get_or_create(old_name)
        self._name_to_id[new_name] = eid
        if new_name not in self._id_to_names[eid]:
            self._id_to_names[eid].append(new_name)
        self._rename_log.append((old_name, new_name))
        return eid

    def canonical_eid(self, eid: str) -> str:
        """The eid (prefix) is already canonical."""
        return eid

    def same_entity(self, eid_a: str, eid_b: str) -> bool:
        """Case-insensitive prefix match."""
        if not eid_a or not eid_b:
            return False
        return eid_a.lower() == eid_b.lower()

    def all_names_for(self, service_name: str) -> List[str]:
        """Return all names this entity has ever had."""
        eid = self._name_to_id.get(service_name)
        if eid is None:
            return [service_name]
        return list(self._id_to_names[eid])

    def canonical_name(self, service_name: str) -> str:
        """Return the most recent name for the entity."""
        eid = self._name_to_id.get(service_name)
        if eid is None:
            return service_name
        names = self._id_to_names[eid]
        return names[-1] if names else service_name

    def are_same_entity(self, name_a: str, name_b: str) -> bool:
        """Check if two names refer to the same logical service."""
        id_a = self._name_to_id.get(name_a)
        id_b = self._name_to_id.get(name_b)
        if id_a is None or id_b is None:
            return name_a == name_b
        return id_a == id_b

    @property
    def rename_log(self) -> List[Tuple[str, str]]:
        return list(self._rename_log)


# ---------------------------------------------------------------------------
# Baseline Statistics — rolling mean/std for anomaly z-scores
# ---------------------------------------------------------------------------


class ServiceBaseline:
    """Rolling statistics for one metric of one service."""

    def __init__(self, window: int = 1000):
        self._window = window
        self._values: deque = deque(maxlen=window)
        self._sum = 0.0
        self._sum_sq = 0.0

    def update(self, value: float):
        if len(self._values) == self._window:
            old = self._values[0]
            self._sum -= old
            self._sum_sq -= old * old
        self._values.append(value)
        self._sum += value
        self._sum_sq += value * value

    @property
    def count(self) -> int:
        return len(self._values)

    @property
    def mean(self) -> float:
        if not self._values:
            return 0.0
        return self._sum / len(self._values)

    @property
    def std(self) -> float:
        n = len(self._values)
        if n < 2:
            return 1.0  # avoid division by zero; treat as 1 unit of noise
        var = (self._sum_sq / n) - (self._sum / n) ** 2

        # Ensure minimum variance to avoid saturation when values are identical.
        # Min std is either 5% of the mean or 1e-2.
        min_var = max(1e-4, (abs(self.mean) * 0.05) ** 2)
        return math.sqrt(max(var, min_var))

    def z_score(self, value: float) -> float:
        return (value - self.mean) / self.std

    def normalized_anomaly(self, value: float) -> float:
        """Map z-score to [0, 1] using a sigmoid-like transform."""
        z = abs(self.z_score(value))
        # sigmoid: 1 / (1 + e^(-k*(z-offset)))
        # z=2 → ~0.73, z=3 → ~0.95, z=0 → ~0.27
        return 1.0 / (1.0 + math.exp(-0.8 * (z - 1.5)))


class BaselineStats:
    """
    Per-service, per-metric rolling baseline.
    Used for computing anomaly scores during reconstruction.
    """

    def __init__(self, window: int = 1000):
        self._window = window
        # (service_entity_id, metric_name) → ServiceBaseline
        self._stats: Dict[Tuple[str, str], ServiceBaseline] = {}

    def update(self, service_eid: str, metric_name: str, value: float):
        key = (service_eid, metric_name)
        if key not in self._stats:
            self._stats[key] = ServiceBaseline(self._window)
        self._stats[key].update(value)

    def get_anomaly_score(
        self, service_eid: str, metric_name: str, value: float
    ) -> float:
        key = (service_eid, metric_name)
        if key not in self._stats or self._stats[key].count < 3:
            return 0.5  # insufficient data → neutral
        return self._stats[key].normalized_anomaly(value)

    def get_baseline(
        self, service_eid: str, metric_name: str
    ) -> Optional[ServiceBaseline]:
        return self._stats.get((service_eid, metric_name))


# ---------------------------------------------------------------------------
# Incident Family Registry — fingerprint storage + fuzzy Levenshtein lookup
# ---------------------------------------------------------------------------


def _levenshtein(s1: str, s2: str) -> int:
    """Standard Levenshtein edit distance."""
    if s1 == s2:
        return 0
    m, n = len(s1), len(s2)
    if m == 0:
        return n
    if n == 0:
        return m
    # Use two-row DP for memory efficiency
    prev = list(range(n + 1))
    curr = [0] * (n + 1)
    for i in range(1, m + 1):
        curr[0] = i
        for j in range(1, n + 1):
            if s1[i - 1] == s2[j - 1]:
                curr[j] = prev[j - 1]
            else:
                curr[j] = 1 + min(prev[j], curr[j - 1], prev[j - 1])
        prev, curr = curr, [0] * (n + 1)
    return prev[n]


def levenshtein_similarity(fp1: str, fp2: str) -> float:
    """Normalized similarity ratio [0, 1]. 1.0 = identical."""
    if not fp1 and not fp2:
        return 1.0
    max_len = max(len(fp1), len(fp2))
    if max_len == 0:
        return 1.0
    dist = _levenshtein(fp1, fp2)
    return 1.0 - dist / max_len


class IncidentFamilyRegistry:
    """
    Stores behavioral fingerprints of past incidents.
    Supports fuzzy lookup by Levenshtein similarity.
    """

    def __init__(self, cache_size: int = 100):
        # incident_id → (fingerprint, ts, service_eid, upstream_eid)
        self._families: Dict[str, Tuple[str, str, str, str]] = {}
        self._cache_size = cache_size
        # Recency cache: most recent cache_size incidents
        self._recent: deque = deque(maxlen=cache_size)

    def register(
        self,
        incident_id: str,
        coarse: str,
        shape: str,
        exact: str,
        ts: str,
        service_eid: str = "",
        upstream_eid: str = "",
    ):
        """Store a fingerprint for an incident."""
        self._families[incident_id] = (
            coarse,
            shape,
            exact,
            ts,
            service_eid,
            upstream_eid,
        )
        self._recent.append(incident_id)

    def find_similar(
        self,
        coarse: str,
        shape: str,
        exact: str,
        current_ts: str = "",
        service_eid: str = "",
        threshold: float = 0.70,
        top_k: int = 5,
    ) -> List[Tuple[str, float, str]]:
        """
        Return list of (incident_id, similarity, stored_fingerprint)
        for all stored incidents with similarity ≥ threshold and
        timestamp < current_ts.
        Sorted by similarity descending.

        Boosts similarity if the service_eid matches.
        """
        if not exact:
            return []

        results = []
        # Prioritize recent cache
        checked = set()
        for iid in reversed(list(self._recent)):
            if iid in self._families:
                p_coarse, p_shape, p_exact, p_ts, past_eid, _up_eid = self._families[iid]
                
                # Prevent matching against future incidents
                if current_ts and p_ts >= current_ts:
                    continue

                # STAGE 2: Deep Re-ranking
                sim = levenshtein_similarity(exact, p_exact)

                # Boost if it's the same logical entity
                if service_eid and past_eid == service_eid:
                    sim = min(1.0, sim * 1.2)

                if sim >= threshold:
                    results.append((iid, sim, p_exact))
                checked.add(iid)

        # Also scan all stored (for older incidents)
        for iid, (
            p_coarse,
            p_shape,
            p_exact,
            p_ts,
            past_eid,
            _up_eid,
        ) in self._families.items():
            if iid in checked:
                continue
            
            # Prevent matching against future incidents
            if current_ts and p_ts >= current_ts:
                continue

            # STAGE 2
            sim = levenshtein_similarity(exact, p_exact)

            if service_eid and past_eid == service_eid:
                sim = min(1.0, sim * 1.2)

            if sim >= threshold:
                results.append((iid, sim, p_exact))

        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    def get(self, incident_id: str) -> Optional[Tuple[str, str]]:
        return self._families.get(incident_id)

    @property
    def size(self) -> int:
        return len(self._families)
