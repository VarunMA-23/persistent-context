"""
rules_engine.py — Four causal rules that fire at ingest time (never on query).

Rule 1: Temporal Proximity  → temporal edges
Rule 2: Correlated Degradation → correlation edges
Rule 3: Deployment Adjacency → deployment edges
Rule 4: Incident Family Extraction → behavioral fingerprints
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from engine.causal_graph import CausalGraph
from engine.incident_fingerprinter import IncidentFingerprinter
from engine.memory_substrate import (
    BaselineStats,
    IncidentFamilyRegistry,
    ServiceIdentity,
)
from engine.schema import CausalEdge, Event, EventKind
from engine.telemetry_buffer import TelemetryBuffer, _parse_ts

# ---------------------------------------------------------------------------
# Confidence decay helper
# ---------------------------------------------------------------------------


def _decay(base_conf: float, delta_sec: float, window_sec: float) -> float:
    if window_sec <= 0:
        return base_conf
    raw = base_conf * (1.0 - delta_sec / window_sec)
    return max(0.0, raw)


# ---------------------------------------------------------------------------
# Base rule
# ---------------------------------------------------------------------------


class Rule:
    name: str = "base"

    def fire(
        self,
        event: Event,
        buffer: TelemetryBuffer,
        graph: CausalGraph,
        identity: ServiceIdentity,
        baseline: BaselineStats,
        family_registry: IncidentFamilyRegistry,
    ) -> List[CausalEdge]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Rule 1: Temporal Proximity
# ---------------------------------------------------------------------------


class TemporalProximityRule(Rule):
    """
    When a metric/log/trace arrives, scan backward up to 50 events.
    Link causally to recent deploy/metric/log within time windows.
    """

    name = "temporal_proximity"

    WINDOWS = {
        # (cause_kind, effect_kind): (window_sec, base_confidence)
        ("deploy", "metric"): (5.0, 0.90),
        ("deploy", "log"): (30.0, 0.75),
        ("deploy", "trace"): (30.0, 0.72),
        ("metric", "log"): (90.0, 0.70),
        ("metric", "trace"): (60.0, 0.68),
        ("log", "metric"): (60.0, 0.65),
        ("log", "trace"): (60.0, 0.63),
    }

    TRIGGER_KINDS = {EventKind.METRIC.value, EventKind.LOG.value, EventKind.TRACE.value}
    CAUSE_KINDS = {EventKind.DEPLOY.value, EventKind.METRIC.value, EventKind.LOG.value}

    def fire(self, event, buffer, graph, identity, baseline, family_registry):
        if event.get("kind") not in self.TRIGGER_KINDS:
            return []

        effect_idx = event.get("_index", -1)
        if effect_idx < 0:
            return []

        effect_ts = _parse_ts(event.get("ts", ""))
        effect_kind = event.get("kind")
        effect_svc = event.get("service", "")

        # Scan backward 60 seconds (time-aware memory)
        edges = []
        lookback_window = 60.0
        potential_causes = buffer.get_range_by_ts(
            effect_ts - lookback_window, 
            effect_ts
        )

        for cause_ev in potential_causes:
            # Skip itself
            if cause_ev.get("_index") == effect_idx:
                continue
            cause_kind = cause_ev.get("kind")
            if cause_kind not in self.CAUSE_KINDS:
                continue

            window_key = (cause_kind, effect_kind)
            if window_key not in self.WINDOWS:
                continue

            cause_ts = _parse_ts(cause_ev.get("ts", ""))
            delta = effect_ts - cause_ts
            if delta < 0:
                continue

            window_sec, base_conf = self.WINDOWS[window_key]
            if delta > window_sec:
                continue

            conf = _decay(base_conf, delta, window_sec)
            if conf <= 0:
                continue

            cause_svc = cause_ev.get("service", cause_ev.get("target", ""))
            cause_idx = cause_ev.get("_index", i)

            edges.append(
                CausalEdge(
                    cause_idx=cause_idx,
                    effect_idx=effect_idx,
                    edge_type="temporal",
                    evidence=(
                        f"{cause_kind}[{cause_svc}@{cause_ev.get('ts', '')}] "
                        f"→ {effect_kind}[{effect_svc}@{event.get('ts', '')}] "
                        f"Δ={delta:.1f}s conf={conf:.2f}"
                    ),
                    confidence=conf,
                    temporal_gap_ms=int(delta * 1000),
                )
            )

        return edges


# ---------------------------------------------------------------------------
# Rule 2: Correlated Degradation
# ---------------------------------------------------------------------------


class CorrelationRule(Rule):
    """
    On each metric event, check Pearson correlation with other services
    over a 5-minute lookback window. ρ > 0.70 → correlation edge.
    """

    name = "correlated_degradation"

    WINDOW_SEC = 300.0  # 5 minutes
    RHO_THRESHOLD = 0.70
    BASE_CONF = 0.65
    MIN_SAMPLES = 5

    def fire(self, event, buffer, graph, identity, baseline, family_registry):
        if event.get("kind") != EventKind.METRIC.value:
            return []

        effect_idx = event.get("_index", -1)
        effect_ts = _parse_ts(event.get("ts", ""))
        effect_svc = event.get("service", "")
        effect_metric = event.get("name", "")

        if not effect_svc or not effect_metric:
            return []

        # Get all metric events in 5-min window
        window_events = buffer.get_by_kind(
            EventKind.METRIC.value,
            start_ts=effect_ts - self.WINDOW_SEC,
            end_ts=effect_ts,
        )

        # Group by service
        svc_values: Dict[str, List[Tuple[float, int, float]]] = defaultdict(list)
        for ev in window_events:
            svc = ev.get("service", "")
            if svc == effect_svc:
                continue
            ts_f = _parse_ts(ev.get("ts", ""))
            val = ev.get("value")
            if val is None:
                continue
            svc_values[svc].append((ts_f, ev.get("_index", 0), float(val)))

        # Get current service values
        cur_events = buffer.get_by_service(
            effect_svc, effect_ts - self.WINDOW_SEC, effect_ts
        )
        cur_metric_vals = [
            (_parse_ts(e.get("ts", "")), float(e.get("value", 0)))
            for e in cur_events
            if e.get("kind") == EventKind.METRIC.value
            and e.get("name") == effect_metric
        ]

        if len(cur_metric_vals) < self.MIN_SAMPLES:
            return []

        edges = []
        for other_svc, other_vals in svc_values.items():
            if len(other_vals) < self.MIN_SAMPLES:
                continue

            # Align by time: bucket into 10-second bins
            rho = self._pearson_correlation(
                cur_metric_vals, [(v[0], v[2]) for v in other_vals]
            )
            if rho is None or abs(rho) < self.RHO_THRESHOLD:
                continue

            # Find earliest correlated event
            other_earliest_idx = min(v[1] for v in other_vals)
            cause_idx = min(other_earliest_idx, effect_idx - 1)
            if cause_idx >= effect_idx:
                continue

            # Confidence decays with time
            delta = effect_ts - min(v[0] for v in other_vals)
            conf = self.BASE_CONF * (1.0 - delta / self.WINDOW_SEC)
            conf = max(0.30, conf)

            edges.append(
                CausalEdge(
                    cause_idx=cause_idx,
                    effect_idx=effect_idx,
                    edge_type="correlation",
                    evidence=(
                        f"Pearson ρ={rho:.2f} between {other_svc} and {effect_svc} "
                        f"over {self.WINDOW_SEC:.0f}s window conf={conf:.2f}"
                    ),
                    confidence=conf,
                    temporal_gap_ms=int(delta * 1000),
                )
            )

        return edges

    @staticmethod
    def _pearson_correlation(
        series_a: List[Tuple[float, float]],
        series_b: List[Tuple[float, float]],
        bin_size: float = 10.0,
    ) -> Optional[float]:
        """Compute Pearson ρ on time-binned series."""
        if not series_a or not series_b:
            return None

        # Bin by floor(ts / bin_size)
        bins_a: Dict[int, List[float]] = defaultdict(list)
        bins_b: Dict[int, List[float]] = defaultdict(list)
        for ts, v in series_a:
            bins_a[int(ts / bin_size)].append(v)
        for ts, v in series_b:
            bins_b[int(ts / bin_size)].append(v)

        common_bins = set(bins_a.keys()) & set(bins_b.keys())
        if len(common_bins) < 3:
            return None

        a_vals = [sum(bins_a[b]) / len(bins_a[b]) for b in sorted(common_bins)]
        b_vals = [sum(bins_b[b]) / len(bins_b[b]) for b in sorted(common_bins)]

        n = len(a_vals)
        mean_a = sum(a_vals) / n
        mean_b = sum(b_vals) / n

        num = sum((a - mean_a) * (b - mean_b) for a, b in zip(a_vals, b_vals))
        den_a = math.sqrt(sum((a - mean_a) ** 2 for a in a_vals))
        den_b = math.sqrt(sum((b - mean_b) ** 2 for b in b_vals))

        if den_a < 1e-9 or den_b < 1e-9:
            return None

        return num / (den_a * den_b)


# ---------------------------------------------------------------------------
# Rule 3: Deployment Adjacency
# ---------------------------------------------------------------------------


class DeploymentAdjacencyRule(Rule):
    """
    On each deploy, scan forward 15 minutes for incident_signal or error log.
    Creates deployment edges.
    """

    name = "deployment_adjacency"

    FORWARD_WINDOW_SEC = 900.0  # 15 minutes
    BASE_CONF = 0.50

    def fire(self, event, buffer, graph, identity, baseline, family_registry):
        if event.get("kind") != EventKind.DEPLOY.value:
            return []

        deploy_idx = event.get("_index", -1)
        deploy_ts = _parse_ts(event.get("ts", ""))
        deploy_svc = event.get("service", "")

        if deploy_idx < 0 or not deploy_svc:
            return []

        # Get all names this service has ever had (handles renames)
        all_names = identity.all_names_for(deploy_svc)

        # Look forward in the buffer from deploy position
        edges = []
        window_end_ts = deploy_ts + self.FORWARD_WINDOW_SEC

        # Scan forward up to 200 events
        for i in range(deploy_idx + 1, min(deploy_idx + 200, buffer.total_count)):
            future_ev = buffer.get_by_index(i)
            if future_ev is None:
                continue

            future_ts = _parse_ts(future_ev.get("ts", ""))
            if future_ts > window_end_ts:
                break  # Events are roughly time-ordered

            future_kind = future_ev.get("kind")
            future_svc = future_ev.get("service", future_ev.get("target", ""))

            # Check if same entity
            is_same_entity = future_svc in all_names or any(
                identity.are_same_entity(deploy_svc, n) for n in [future_svc]
            )

            is_incident = future_kind == EventKind.INCIDENT_SIGNAL.value
            is_error = (
                future_kind == EventKind.LOG.value and future_ev.get("level") == "error"
            )

            if not (is_incident or is_error):
                continue

            # Only link if same service/entity OR if incident has no service filter
            if not is_same_entity and not is_incident:
                continue

            delta = future_ts - deploy_ts
            conf = _decay(self.BASE_CONF, delta, self.FORWARD_WINDOW_SEC)
            if conf <= 0:
                continue

            future_idx = future_ev.get("_index", i)
            if future_idx <= deploy_idx:
                continue

            edges.append(
                CausalEdge(
                    cause_idx=deploy_idx,
                    effect_idx=future_idx,
                    edge_type="deployment",
                    evidence=(
                        f"Deploy {deploy_svc} v{event.get('version', '')} "
                        f"→ {future_kind}[{future_svc}] "
                        f"Δ={delta:.1f}s conf={conf:.2f}"
                    ),
                    confidence=conf,
                    temporal_gap_ms=int(delta * 1000),
                )
            )

        return edges


# ---------------------------------------------------------------------------
# Rule 4: Incident Family Extraction
# ---------------------------------------------------------------------------


class IncidentFamilyRule(Rule):
    """
    On each incident_signal, extract the preceding event sequence,
    generate a behavioral fingerprint, and register it.
    """

    name = "incident_family"

    LOOKBACK_SEC = 2100.0  # 35 minutes (bench incidents deploy ~30m before signal)

    @staticmethod
    def _service_from_trigger(trigger: str) -> str:
        if not trigger or ":" not in trigger:
            return ""
        part = trigger.split(":", 1)[1]
        return part.split("/")[0] if "/" in part else part

    def fire(self, event, buffer, graph, identity, baseline, family_registry):
        if event.get("kind") != EventKind.INCIDENT_SIGNAL.value:
            return []

        incident_id = event.get("incident_id", "")
        incident_ts = _parse_ts(event.get("ts", ""))
        incident_idx = event.get("_index", -1)

        if not incident_id or incident_idx < 0:
            return []

        # Gather events in 10-min lookback window
        lookback_events = buffer.get_range_by_ts(
            incident_ts - self.LOOKBACK_SEC,
            incident_ts,
        )

        svc = event.get("service", "") or self._service_from_trigger(
            event.get("trigger", "")
        )
        all_names = identity.all_names_for(svc) if svc else []
        coarse, shape, exact = IncidentFingerprinter.signature_fingerprint(
            lookback_events, all_names, identity=identity
        )

        sig_events = IncidentFingerprinter.extract_signature_events(
            lookback_events, all_names
        )
        upstream_eid = ""
        for ev in sig_events:
            if ev.get("kind") == EventKind.LOG.value and ev.get("level") == "error":
                up_svc = ev.get("service", "")
                if up_svc:
                    upstream_eid = identity.canonical_eid(identity.resolve(up_svc))

        service_eid = identity.resolve(svc) if svc else ""
        family_registry.register(
            incident_id,
            coarse,
            shape,
            exact,
            event.get("ts", ""),
            identity.canonical_eid(service_eid) if service_eid else "",
            upstream_eid=upstream_eid,
        )

        # No causal edges generated here — fingerprinting is for matching, not causality
        return []


# ---------------------------------------------------------------------------
# Rules Engine — orchestrates all rules
# ---------------------------------------------------------------------------


class RulesEngine:
    """
    Fires all rules on each ingested event.
    Rules run in priority order: temporal → deployment → correlation → family.
    """

    def __init__(
        self,
        buffer: TelemetryBuffer,
        graph: CausalGraph,
        identity: ServiceIdentity,
        baseline: BaselineStats,
        family_registry: IncidentFamilyRegistry,
    ):
        self._buffer = buffer
        self._graph = graph
        self._identity = identity
        self._baseline = baseline
        self._family_registry = family_registry

        self._rules: List[Rule] = [
            TemporalProximityRule(),
            DeploymentAdjacencyRule(),
            CorrelationRule(),
            IncidentFamilyRule(),
        ]

    def process(self, event: Event):
        """Fire all applicable rules for the given event."""
        kind = event.get("kind", "")

        # Pre-process: update identity and baseline
        svc = event.get("service", "")
        if svc:
            self._identity.get_or_create(svc)

        if kind == EventKind.TOPOLOGY.value:
            change = event.get("change", "")
            from_name = (
                event.get("from_name") or event.get("from_") or event.get("from") or ""
            )
            to_name = event.get("to_name") or event.get("to", "") or ""
            if change == "rename" and from_name and to_name:
                self._identity.on_rename(from_name, to_name)

        if kind == EventKind.METRIC.value:
            svc_eid = self._identity.resolve(svc) if svc else svc
            metric_name = event.get("name", "")
            val = event.get("value")
            if val is not None and metric_name:
                self._baseline.update(svc_eid, metric_name, float(val))

        # Fire each rule
        for rule in self._rules:
            try:
                edges = rule.fire(
                    event,
                    self._buffer,
                    self._graph,
                    self._identity,
                    self._baseline,
                    self._family_registry,
                )
                for edge in edges:
                    self._graph.add_edge(edge)
            except Exception:
                # Never let a rule failure break ingestion
                pass
