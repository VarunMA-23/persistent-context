"""
incident_fingerprinter.py — Behavioral event abstraction and fingerprinting.

Converts raw event sequences into topology-invariant fingerprint strings.
Uses Levenshtein similarity for fuzzy family matching.
"""

from __future__ import annotations

from typing import Any, Iterable, List, Optional, Set, Tuple

from engine.schema import Event, EventKind

# ---------------------------------------------------------------------------
# Event type → single character code
# ---------------------------------------------------------------------------

_KIND_CODE = {
    EventKind.DEPLOY.value: "D",
    EventKind.METRIC.value: "M",  # degradation (overridden below)
    EventKind.LOG.value: "L",
    EventKind.TRACE.value: "T",
    EventKind.INCIDENT_SIGNAL.value: "I",
    EventKind.REMEDIATION.value: "R",
    EventKind.TOPOLOGY.value: "C",
}

_METRIC_DEGRADATION_THRESHOLD = 1000  # ms latency, or generic high threshold


class IncidentFingerprinter:
    """
    Stateless fingerprint generator.

    Converts event sequences into abstract strings like "DMER" that
    capture behavioral shape independent of service names or metric values.
    """

    @staticmethod
    def event_to_code(event: Event) -> str:
        """Map a single event to a single-character behavioral code."""
        kind = event.get("kind", "")

        if kind == EventKind.METRIC.value:
            val = event.get("value")
            name = event.get("name", "").lower()
            if val is not None:
                # Context-aware degradation detection
                is_deg = False
                if "latency" in name or "duration" in name or "response_time" in name:
                    is_deg = float(val) > 500
                    return "A" if is_deg else "a"
                if "error" in name or "failure" in name:
                    is_deg = float(val) > 0
                    return "X" if is_deg else "x"
                if "throughput" in name or "rps" in name or "rate" in name:
                    is_deg = float(val) < 50
                    return "B" if is_deg else "b"
                if "cpu" in name or "mem" in name or "util" in name:
                    is_deg = float(val) > 80
                    return "U" if is_deg else "u"

                # Generic: high value = degradation signal
                is_deg = float(val) > _METRIC_DEGRADATION_THRESHOLD
                return "M" if is_deg else "m"
            return "M"

        if kind == EventKind.LOG.value:
            level = event.get("level", "").lower()
            if level == "error":
                return "E"
            if level == "warn":
                return "W"
            return "L"

        if kind == EventKind.REMEDIATION.value:
            outcome = event.get("outcome", "")
            if outcome == "resolved":
                return "R"
            if outcome == "partial":
                return "P"
            if outcome == "failed":
                return "F"
            return "R"

        return _KIND_CODE.get(kind, "?")

    @staticmethod
    def _metric_signal_score(ev: Event) -> float:
        """Score a metric event by how likely it represents a real degradation.

        Higher score = more anomalous / more interesting as an incident signal.
        This ensures latency spikes beat background QPS readings.
        """
        name = ev.get("name", "").lower()
        val = ev.get("value")
        if val is None:
            return 0.0
        val_f = float(val)

        # Latency/duration spikes are the highest-signal metrics
        if "latency" in name or "duration" in name or "response_time" in name:
            return 3.0 + min(val_f / 1000.0, 10.0)  # 3.0 base + scaled value
        # Error/failure rate spikes
        if "error" in name or "failure" in name:
            return 2.5 + min(val_f, 5.0) if val_f > 0 else 0.0
        # Resource utilization spikes
        if "cpu" in name or "mem" in name or "util" in name:
            return 1.5 + (val_f / 100.0) if val_f > 80 else 0.1
        # Throughput drops (low value = bad)
        if "throughput" in name or "rps" in name or "qps" in name:
            return 0.1  # background noise — never interesting as a signal
        # Generic high metric value
        return 0.5 + min(val_f / 5000.0, 2.0) if val_f > 1000 else 0.1

    @staticmethod
    def extract_signature_events(
        events: List[Event],
        service_names: Optional[Iterable[str]] = None,
    ) -> List[Event]:
        """
        Pull the canonical deploy → degradation → upstream-error pattern
        from a noisy window. Topology-invariant (uses service_names set).

        Keeps the most recent deploy, the MOST anomalous metric (not just
        the last one), and the most recent error log.
        """
        if not events:
            return []

        names: Optional[Set[str]] = set(service_names) if service_names else None
        sorted_events = sorted(
            events,
            key=lambda e: (e.get("ts", ""), e.get("_index", 0)),
        )

        def _in_scope(ev: Event) -> bool:
            if names is None:
                return True
            svc = ev.get("service") or ev.get("target") or ""
            if svc in names:
                return True
            if ev.get("kind") == EventKind.LOG.value:
                msg = ev.get("msg", "")
                return any(n in msg for n in names)
            return False

        scoped = [e for e in sorted_events if _in_scope(e)]
        if not scoped:
            scoped = sorted_events

        deploy: Optional[Event] = None
        best_metric: Optional[Event] = None
        best_metric_score: float = -1.0
        error_log: Optional[Event] = None

        for ev in scoped:
            kind = ev.get("kind")
            if kind == EventKind.DEPLOY.value:
                deploy = ev
            elif kind == EventKind.METRIC.value:
                # Keep the MOST anomalous metric, not just the last one.
                # A latency spike should never be overwritten by a background QPS reading.
                score = IncidentFingerprinter._metric_signal_score(ev)
                if score > best_metric_score:
                    best_metric_score = score
                    best_metric = ev
            elif kind == EventKind.LOG.value and ev.get("level", "").lower() == "error":
                error_log = ev

        signature: List[Event] = []
        if deploy:
            signature.append(deploy)
        if best_metric:
            signature.append(best_metric)
        if error_log:
            signature.append(error_log)
        return signature

    @staticmethod
    def fingerprint(events: List[Event]) -> str:
        """
        Generate a behavioral fingerprint string from an event sequence.
        Events should be in temporal order.
        Deduplicates consecutive identical codes to avoid inflation.
        """
        if not events:
            return ""

        # Sort by timestamp / buffer index
        sorted_events = sorted(
            events, key=lambda e: (e.get("ts", ""), e.get("_index", 0))
        )

        codes = []
        for ev in sorted_events:
            code = IncidentFingerprinter.event_to_code(ev)
            if code in (
                "?",
                "m",
                "T",
                "C",
                "I",
            ):  # skip unknowns, noise, and the signal itself
                continue
            # Light deduplication: don't repeat the same code more than 3 times in a row
            if len(codes) >= 3 and all(c == code for c in codes[-3:]):
                continue
            codes.append(code)

        return "".join(codes)

    @staticmethod
    def _entity_code(identity: Any, service_name: str) -> str:
        """Single-letter topology-stable code for a service entity."""
        if not service_name or identity is None:
            return ""
        eid = identity.canonical_eid(identity.resolve(service_name))
        bucket = sum(ord(c) for c in eid) % 26
        return chr(ord("A") + bucket)

    @staticmethod
    def signature_fingerprint(
        events: List[Event],
        service_names: Optional[Iterable[str]] = None,
        identity: Any = None,
    ) -> Tuple[str, str, str]:
        """
        Fingerprint the canonical deploy → degradation → error pattern.

        Returns (coarse_archetype, behavioral_shape, exact_sequence).
        """
        sig_events = IncidentFingerprinter.extract_signature_events(
            events, service_names
        )
        if not sig_events:
            return ("unknown", "", "")

        codes: List[str] = []
        for ev in sig_events:
            code = IncidentFingerprinter.event_to_code(ev)
            if code in ("?", "m", "T", "C", "I"):
                continue
            codes.append(code)

        exact = "".join(codes)

        # Determine Coarse Archetype
        if "D" in exact and "E" in exact:
            coarse = "deploy_failure"
        elif "a" in exact and "X" in exact:
            coarse = "upstream_cascade"
        elif "E" in exact:
            coarse = "error_spike"
        else:
            coarse = "degradation_generic"

        # Determine Behavioral Shape (strip entity codes)
        shape = "".join([c[0] for c in exact])  # E2 => E

        return (coarse, shape, exact)

    @staticmethod
    def order_invariant_key(fingerprint: str) -> str:
        """Sort codes so DAE and DEA compare equal."""
        return "".join(sorted(fingerprint))

    @staticmethod
    def similarity(fp1: str, fp2: str) -> float:
        """
        Compute Levenshtein similarity ratio [0, 1].
        Imported here for convenience; canonical implementation in memory_substrate.
        """
        from engine.memory_substrate import levenshtein_similarity

        return levenshtein_similarity(fp1, fp2)

    @staticmethod
    def describe(fingerprint: str) -> str:
        """Human-readable description of a fingerprint."""
        _desc = {
            "D": "deployment",
            "A": "latency-spike",
            "B": "throughput-drop",
            "U": "utilization-high",
            "X": "error-rate-high",
            "M": "metric-degradation",
            "a": "latency-low",
            "b": "throughput-ok",
            "u": "utilization-ok",
            "x": "error-rate-ok",
            "m": "metric-ok",
            "E": "error-log",
            "W": "warning-log",
            "L": "log-event",
            "T": "trace",
            "I": "incident-signal",
            "R": "remediation-resolved",
            "P": "remediation-partial",
            "F": "remediation-failed",
            "C": "topology-change",
        }
        parts = [_desc.get(c, c) for c in fingerprint]
        return " → ".join(parts) if parts else "(empty)"
