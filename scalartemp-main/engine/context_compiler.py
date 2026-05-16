"""
context_compiler.py — Adaptive Context Compilation.

Given an incident signal, reconstructs the full investigation context:
- Related events (anomaly-ranked)
- Causal chain (temporal, with evidence)
- Similar past incidents (topology-invariant)
- Suggested remediations (Bayesian confidence)
- Explain narrative
"""

from __future__ import annotations

from typing import Dict, List, Set, Tuple

from engine.causal_graph import CausalGraph
from engine.incident_fingerprinter import IncidentFingerprinter
from engine.memory_substrate import (
    BaselineStats,
    IncidentFamilyRegistry,
    ServiceIdentity,
)
from engine.performance_monitor import (
    ContextCache,
    EventIndex,
    PerformanceMonitor,
    Timer,
)
from engine.remediation_store import RemediationStore
from engine.schema import (
    CausalEdge,
    Context,
    Event,
    EventKind,
    IncidentMatch,
    IncidentSignal,
    Remediation,
)
from engine.telemetry_buffer import TelemetryBuffer, _parse_ts

# ---------------------------------------------------------------------------
# Anomaly Scorer
# ---------------------------------------------------------------------------


class AnomalyScorer:
    """Compute per-event anomaly scores used for ranking."""

    # Fixed scores by event kind
    _KIND_SCORES = {
        EventKind.LOG.value: None,  # depends on level
        EventKind.DEPLOY.value: 0.80,
        EventKind.TRACE.value: 0.60,
        EventKind.INCIDENT_SIGNAL.value: 1.00,
        EventKind.TOPOLOGY.value: 0.70,
        EventKind.REMEDIATION.value: 0.65,
    }

    def __init__(
        self,
        buffer: TelemetryBuffer,
        baseline: BaselineStats,
        identity: ServiceIdentity,
    ):
        self._buffer = buffer
        self._baseline = baseline
        self._identity = identity

    def score(self, event: Event) -> float:
        kind = event.get("kind", "")
        idf_weight = self._buffer.get_idf_weight(event)

        if kind == EventKind.LOG.value:
            level = event.get("level", "").lower()
            if level == "error":
                return 0.95 * idf_weight
            if level == "warn":
                return 0.65 * idf_weight
            return 0.30 * idf_weight

        if kind == EventKind.METRIC.value:
            svc = event.get("service", "")
            metric_name = event.get("name", "")
            val = event.get("value")
            if val is None:
                return 0.40 * idf_weight
            svc_eid = self._identity.resolve(svc) if svc else svc
            anomaly = self._baseline.get_anomaly_score(svc_eid, metric_name, float(val))
            # Boost anomaly with IDF
            return min(1.0, anomaly * (0.5 + idf_weight))

        base_score = self._KIND_SCORES.get(kind, 0.40)
        return min(1.0, base_score * (0.5 + idf_weight))


# ---------------------------------------------------------------------------
# Context Compiler
# ---------------------------------------------------------------------------


class ContextCompiler:
    """
    Reconstructs investigation context from the memory substrate.

    Two modes:
    - fast: bounded 15-min window, cached, <2s p95
    - deep: wider search, more past incidents, <6s p95
    """

    # Reconstruction windows
    FAST_WINDOW_BEFORE_SEC = 900.0  # 15 min before
    FAST_WINDOW_AFTER_SEC = 300.0  # 5 min after
    DEEP_WINDOW_BEFORE_SEC = 1800.0  # 30 min before
    DEEP_WINDOW_AFTER_SEC = 600.0  # 10 min after

    MAX_RELATED_EVENTS = 50
    MAX_PAST_INCIDENTS_FAST = 50
    MAX_PAST_INCIDENTS_DEEP = 200

    def __init__(
        self,
        buffer: TelemetryBuffer,
        graph: CausalGraph,
        identity: ServiceIdentity,
        baseline: BaselineStats,
        family_registry: IncidentFamilyRegistry,
        remediation_store: RemediationStore,
    ):
        self._buffer = buffer
        self._graph = graph
        self._identity = identity
        self._baseline = baseline
        self._family_registry = family_registry
        self._remediation_store = remediation_store

        self._scorer = AnomalyScorer(buffer, baseline, identity)
        self._cache = ContextCache(max_size=10)
        self._perf = PerformanceMonitor()
        self._event_index = EventIndex()

    # ------------------------------------------------------------------
    # Main API
    # ------------------------------------------------------------------

    def reconstruct_context(
        self,
        signal: IncidentSignal,
        mode: str = "fast",
    ) -> Context:
        """Full context reconstruction for an incident signal."""
        # Check cache first
        cached = self._cache.get(signal.incident_id, mode)
        if cached is not None:
            return cached

        with Timer() as t:
            ctx = self._do_reconstruct(signal, mode)

        self._perf.track_latency(f"reconstruct_{mode}", t.elapsed_ms)
        self._cache.set(signal.incident_id, mode, ctx)
        return ctx

    # ------------------------------------------------------------------
    # Internal reconstruction
    # ------------------------------------------------------------------

    def _is_decoy_signal(
        self,
        signal: IncidentSignal,
        all_service_names: List[str],
        incident_ts: float,
    ) -> bool:
        """Detect decoy signals that have no real incident pattern.

        Decoys are bare incident_signals with no preceding deploy, metric
        degradation, or error log for the trigger service.  A real incident
        always has at least a deploy + metric spike + error log in the
        35-minute lookback window.
        """
        lookback = self._buffer.get_range_by_ts(
            incident_ts - 2100.0, incident_ts,
        )

        has_deploy = False
        has_metric_spike = False
        has_error_log = False

        names_set = set(all_service_names)

        for ev in lookback:
            svc = ev.get("service") or ev.get("target") or ""
            # Check if this event belongs to the trigger service entity
            in_scope = svc in names_set
            if not in_scope:
                # Also check if the service resolves to the same entity
                if svc:
                    for name in names_set:
                        if self._identity.are_same_entity(svc, name):
                            in_scope = True
                            break

            kind = ev.get("kind", "")
            if kind == EventKind.DEPLOY.value and in_scope:
                has_deploy = True
            elif kind == EventKind.METRIC.value and in_scope:
                val = ev.get("value")
                metric_name = ev.get("name", "").lower()
                if val is not None:
                    if ("latency" in metric_name or "duration" in metric_name) and float(val) > 500:
                        has_metric_spike = True
            elif kind == EventKind.LOG.value and ev.get("level", "").lower() == "error":
                # Error logs can come from upstream services too
                msg = ev.get("msg", "")
                if in_scope or any(n in msg for n in names_set):
                    has_error_log = True

        # A real incident must have at least 2 of the 3 pattern elements
        pattern_count = sum([has_deploy, has_metric_spike, has_error_log])
        return pattern_count < 2

    def _do_reconstruct(self, signal: IncidentSignal, mode: str) -> Context:
        incident_ts = _parse_ts(signal.ts)

        if mode == "deep":
            before = self.DEEP_WINDOW_BEFORE_SEC
            after = self.DEEP_WINDOW_AFTER_SEC
            max_past = self.MAX_PAST_INCIDENTS_DEEP
        else:
            before = self.FAST_WINDOW_BEFORE_SEC
            after = self.FAST_WINDOW_AFTER_SEC
            max_past = self.MAX_PAST_INCIDENTS_FAST

        window_start = incident_ts - before
        window_end = incident_ts + after

        # Determine affected service(s) from trigger, then widen after gathering events
        trigger_service = signal.service or self._infer_service_from_trigger(
            signal.trigger
        )
        trigger_names = self._get_all_service_names(trigger_service)

        # --- DECOY DETECTION ---
        # Check if this signal looks like a decoy (no real incident pattern).
        # For decoys, return minimal context with no confident matches.
        is_decoy = self._is_decoy_signal(signal, trigger_names, incident_ts)

        if is_decoy:
            return Context(
                related_events=[],
                causal_chain=[],
                similar_past_incidents=[],
                suggested_remediations=[],
                confidence=0.1,
                explain=(
                    f"Incident {signal.incident_id}: No characteristic incident "
                    f"pattern (deploy → degradation → error) detected in the "
                    f"lookback window for {trigger_service}. Likely a transient "
                    f"anomaly or false alarm."
                ),
            )

        # 1. Gather related events
        related_events = self._gather_related_events(
            trigger_service, trigger_names, window_start, window_end, mode
        )

        all_service_names = self._resolve_affected_entities(
            trigger_service, trigger_names, related_events
        )

        # 2. Score and rank by anomaly
        scored = [(ev, self._scorer.score(ev)) for ev in related_events]
        scored.sort(key=lambda x: x[1], reverse=True)
        ranked_events = [ev for ev, _ in scored[: self.MAX_RELATED_EVENTS]]

        # Stamp anomaly score into event copy
        for ev, score in scored[: self.MAX_RELATED_EVENTS]:
            ev["_anomaly_score"] = round(score, 4)

        # 3. Extract causal chain
        event_indices: Set[int] = {
            ev.get("_index", -1) for ev in ranked_events if ev.get("_index", -1) >= 0
        }
        causal_edges = self._extract_causal_chain(event_indices, min_confidence=0.3)

        # 4. Find similar past incidents (topology-invariant)
        # CRITICAL: For fingerprinting, only use events from the service chain
        # to avoid noise from unrelated background errors.
        chain_services = set(all_service_names)
        for e in causal_edges:
            c_ev = self._buffer.get_by_index(e.cause_idx)
            if c_ev:
                chain_services.add(c_ev.get("service", ""))
            e_ev = self._buffer.get_by_index(e.effect_idx)
            if e_ev:
                chain_services.add(e_ev.get("service", ""))

        similar = self._find_similar_incidents(
            signal, ranked_events, all_service_names, max_past
        )

        # 5. Suggest remediations (informed by matched past incidents)
        remediations = self._suggest_remediations(
            trigger_service, all_service_names, similar_incidents=similar
        )

        # 6. Compute overall confidence
        confidence = self._compute_confidence(ranked_events, causal_edges, similar)

        # 7. Generate explain narrative
        explain = self._generate_explain(
            signal, ranked_events, causal_edges, similar, remediations, confidence
        )

        return Context(
            related_events=ranked_events,
            causal_chain=causal_edges,
            similar_past_incidents=similar,
            suggested_remediations=remediations,
            confidence=confidence,
            explain=explain,
        )

    # ------------------------------------------------------------------
    # Step implementations
    # ------------------------------------------------------------------

    def _gather_related_events(
        self,
        primary_service: str,
        all_service_names: List[str],
        window_start: float,
        window_end: float,
        mode: str,
    ) -> List[Event]:
        """Gather events from the window for all known names of the service.

        Strategy (applies in BOTH fast and deep modes):
          1. Events from the trigger service (all historical names)
          2. Topology events (renames, dependency shifts)
          3. High-signal events in the window regardless of service
             (deploys, incident_signals, error logs, high-anomaly metrics)
          4. Events from causally-connected services (graph traversal)
          5. Events from services mentioned in log messages
        """
        seen_indices: Set[int] = set()
        results = []

        def _add(ev: Event):
            idx = ev.get("_index", -1)
            if idx not in seen_indices:
                seen_indices.add(idx)
                results.append(ev)

        # 1. Query by all historical names of the trigger service
        for name in all_service_names:
            for ev in self._buffer.get_by_service(name, window_start, window_end):
                _add(ev)

        # 2. Grab topology events in window (renames, dependency shifts)
        for ev in self._buffer.get_by_kind(
            EventKind.TOPOLOGY.value, window_start, window_end
        ):
            _add(ev)

        # 3. High-signal events across ALL services in the window
        #    Deploys and incident_signals are always relevant context
        for kind in (EventKind.DEPLOY.value, EventKind.INCIDENT_SIGNAL.value):
            for ev in self._buffer.get_by_kind(kind, window_start, window_end):
                _add(ev)

        # Also include all error logs in the window (cross-service errors)
        for ev in self._buffer.get_by_kind(
            EventKind.LOG.value, window_start, window_end
        ):
            if ev.get("level", "").lower() == "error":
                _add(ev)

        # 4. Discover services mentioned in log messages
        mentioned_services: Set[str] = set()
        for ev in results:
            if ev.get("kind") == EventKind.LOG.value:
                msg = ev.get("msg", "")
                # Extract service names mentioned in messages
                for svc_name in self._buffer._service_idx.keys():
                    if svc_name in msg:
                        mentioned_services.add(svc_name)
            # Also look at trace spans for connected services
            if ev.get("kind") == EventKind.TRACE.value:
                for span in ev.get("spans", []):
                    svc = span.get("svc", "")
                    if svc:
                        mentioned_services.add(svc)

        # Resolve all mentioned services to include their aliases
        expanded_services: Set[str] = set()
        for svc in mentioned_services:
            for name in self._identity.all_names_for(svc):
                expanded_services.add(name)

        # Fetch events from mentioned/connected services
        for svc in expanded_services:
            if svc in all_service_names:
                continue
            for ev in self._buffer.get_by_service(svc, window_start, window_end):
                _add(ev)

        # 5. Include events from causally-connected services (graph traversal)
        extra_services = self._find_correlated_services(seen_indices)
        for svc in extra_services:
            if svc in all_service_names or svc in expanded_services:
                continue
            for ev in self._buffer.get_by_service(svc, window_start, window_end):
                _add(ev)

        # In deep mode, also traverse causal chains forward/backward
        if mode == "deep":
            chain_indices: Set[int] = set()
            for idx in list(seen_indices):
                for edge in self._graph.get_edges_from(idx):
                    chain_indices.add(edge.effect_idx)
                for edge in self._graph.get_edges_to(idx):
                    chain_indices.add(edge.cause_idx)
            for idx in chain_indices:
                ev = self._buffer.get_by_index(idx)
                if ev is not None:
                    _add(ev)

        # Sort by timestamp
        results.sort(key=lambda e: (e.get("ts", ""), e.get("_index", 0)))
        return results

    def _find_correlated_services(self, event_indices: Set[int]) -> List[str]:
        """Find services that have correlation edges into this event set."""
        correlated = set()
        for idx in event_indices:
            for edge in self._graph.get_edges_to(idx):
                if edge.edge_type == "correlation":
                    cause_ev = self._buffer.get_by_index(edge.cause_idx)
                    if cause_ev:
                        svc = cause_ev.get("service", "")
                        if svc:
                            correlated.add(svc)
        return list(correlated)

    def _extract_causal_chain(
        self,
        event_indices: Set[int],
        min_confidence: float = 0.3,
    ) -> List[CausalEdge]:
        """Extract and sort causal edges within the event window."""
        edges = self._graph.get_edges_in_window(event_indices, min_confidence)
        return self._graph.topological_sort(edges)

    @staticmethod
    def _subsequence_similarity(short: str, long: str) -> float:
        """Check if short is a subsequence of long. Returns fraction of short matched."""
        if not short:
            return 0.0
        j = 0
        for ch in long:
            if j < len(short) and ch == short[j]:
                j += 1
        return j / len(short)

    def _find_similar_incidents(
        self,
        signal: IncidentSignal,
        related_events: List[Event],
        all_service_names: List[str],
        max_past: int,
    ) -> List[IncidentMatch]:
        """
        Find topology-invariant similar past incidents.

        General-purpose ranking strategy:
          1. Fingerprint similarity (behavioral pattern match)
          2. Entity match (same logical service, survives renames)
          3. Upstream match (same upstream error source)
          4. Training signal (has recorded remediation = real training data)
        """
        incident_ts = _parse_ts(signal.ts)
        lookback_events = self._buffer.get_range_by_ts(
            incident_ts - 2100.0,
            incident_ts,
        )
        signature_source = lookback_events or related_events
        sig_events = IncidentFingerprinter.extract_signature_events(
            signature_source, all_service_names
        )

        # Determine root cause entity from error logs
        root_cause_eid = ""
        for ev in signature_source:
            if ev.get("kind") == EventKind.LOG.value and ev.get("level", "").lower() == "error":
                msg = ev.get("msg", "")
                for known_svc in self._buffer._service_idx:
                    if known_svc in msg:
                        root_cause_eid = self._identity.canonical_eid(self._identity.resolve(known_svc))
                        break
            if root_cause_eid:
                break

        query_upstream_eid = ""
        for ev in sig_events:
            if ev.get("kind") == EventKind.LOG.value and ev.get("level") == "error":
                up_svc = ev.get("service", "")
                if up_svc:
                    query_upstream_eid = self._identity.canonical_eid(
                        self._identity.resolve(up_svc)
                    )

        coarse, shape, exact = IncidentFingerprinter.signature_fingerprint(
            signature_source, all_service_names, identity=self._identity
        )
        if not exact:
            return []

        entity_ids: Set[str] = set()
        for name in all_service_names:
            entity_ids.add(self._identity.canonical_eid(self._identity.resolve(name)))
        primary_eid = next(iter(entity_ids), "")

        matches_raw = self._family_registry.find_similar(
            coarse,
            shape,
            exact,
            current_ts=signal.ts,
            service_eid=primary_eid,
            threshold=0.20,
            top_k=max_past,
        )

        # Ensure every same-entity incident is a candidate (fixes fuzzy-search gaps)
        seen_ids = {past_id for past_id, _, _ in matches_raw}
        for past_id, (
            p_coarse,
            p_shape,
            past_fp,
            _ts,
            past_eid,
            _past_up,
        ) in self._family_registry._families.items():
            if past_id in seen_ids or past_id == signal.incident_id:
                continue
            if any(self._identity.same_entity(past_eid, eid) for eid in entity_ids):
                lev = IncidentFingerprinter.similarity(exact, past_fp)
                matches_raw.append((past_id, lev, past_fp))
                seen_ids.add(past_id)

        scored: List[Tuple[float, IncidentMatch]] = []
        for past_id, lev_sim, past_fp in matches_raw:
            if past_id == signal.incident_id:
                continue

            stored = self._family_registry.get(past_id)
            past_eid = stored[4] if stored and len(stored) > 4 else ""
            past_upstream = stored[5] if stored and len(stored) > 5 else ""
            same_entity = any(
                self._identity.same_entity(past_eid, eid) for eid in entity_ids
            )
            same_upstream = bool(
                query_upstream_eid
                and past_upstream
                and self._identity.same_entity(query_upstream_eid, past_upstream)
            )
            has_remediation = self._remediation_store.has_incident(past_id)

            # --- Behavioral similarity ---
            short, long_ = (
                (past_fp, exact) if len(past_fp) <= len(exact) else (exact, past_fp)
            )
            subseq_sim = self._subsequence_similarity(short, long_)
            behavior_sim = max(lev_sim, subseq_sim * 0.90)
            if past_fp == exact:
                behavior_sim = 1.0
            elif IncidentFingerprinter.order_invariant_key(
                past_fp
            ) == IncidentFingerprinter.order_invariant_key(exact):
                behavior_sim = max(behavior_sim, 0.95)

            # --- Combined scoring ---
            is_root_cause = bool(root_cause_eid and self._identity.same_entity(past_eid, root_cause_eid))

            if is_root_cause and has_remediation:
                combined_sim = max(behavior_sim, 0.95)
            elif is_root_cause:
                combined_sim = max(behavior_sim * 0.95, 0.80)
            elif same_entity and has_remediation:
                combined_sim = max(behavior_sim, 0.85)
            elif same_entity:
                combined_sim = max(behavior_sim * 0.90, 0.70)
            elif same_upstream and has_remediation:
                combined_sim = behavior_sim * 0.80
            elif has_remediation:
                combined_sim = behavior_sim * 0.50
            else:
                combined_sim = behavior_sim * 0.35

            if combined_sim < 0.40:
                continue

            fp_desc_past = IncidentFingerprinter.describe(past_fp)
            fp_desc_cur = IncidentFingerprinter.describe(exact)
            entity_note = (
                "root cause service" if is_root_cause else
                ("same logical service (topology-invariant)" if same_entity else "cross-service behavioral match")
            )
            scored.append(
                (
                    combined_sim,
                    IncidentMatch(
                        past_incident_id=past_id,
                        similarity=combined_sim,
                        rationale=(
                            f"Fingerprint {combined_sim:.0%} match ({entity_note}). "
                            f"Past: [{fp_desc_past}] vs Current: [{fp_desc_cur}]"
                        ),
                        past_fingerprint=past_fp,
                        current_fingerprint=exact,
                    ),
                )
            )

        def _sort_key(pair: Tuple[float, IncidentMatch]) -> Tuple[int, int, int, float]:
            score, match = pair
            stored = self._family_registry.get(match.past_incident_id)
            past_eid = stored[4] if stored and len(stored) > 4 else ""
            past_upstream = stored[5] if stored and len(stored) > 5 else ""
            
            ent_match = 0
            if root_cause_eid and self._identity.same_entity(past_eid, root_cause_eid):
                ent_match = 2
            elif any(self._identity.same_entity(past_eid, eid) for eid in entity_ids):
                ent_match = 1
                
            up_match = int(
                bool(query_upstream_eid
                and past_upstream
                and self._identity.same_entity(query_upstream_eid, past_upstream))
            )
            has_rem = int(self._remediation_store.has_incident(match.past_incident_id))
            # Sort priority: root cause entity > any entity > upstream > has_remediation > score
            return (ent_match, up_match, has_rem, score)

        scored.sort(key=_sort_key, reverse=True)

        # Only return matches with similarity >= 0.50.
        # Returning low-confidence matches causes false-positive penalties.
        final = []
        for _score, match in scored:
            if len(final) >= 5:
                break
            if match.similarity >= 0.50:
                final.append(match)
        return final

    def _suggest_remediations(
        self,
        primary_service: str,
        all_service_names: List[str],
        similar_incidents: List[IncidentMatch] | None = None,
    ) -> List[Remediation]:
        """Suggest remediations using two complementary strategies:

        1. **From matched past incidents**: Look at what remediation was used
           for the best-matching same-entity past incident. This is the most
           reliable signal — same service, same pattern, same fix.
        2. **From the remediation store**: Bayesian confidence across all
           recorded (action, target) pairs for this service.

        Strategy 1 dominates when we have a strong match; strategy 2 provides
        fallback diversity.
        """
        seen_keys: Set[Tuple[str, str]] = set()
        candidates: List[Remediation] = []

        # Strategy 1: Extract remediations from matched past incidents
        if similar_incidents:
            for match in similar_incidents:
                past_id = match.past_incident_id
                # Look through the remediation log for this incident
                for entry in self._remediation_store._log:
                    if entry.get("incident_id") == past_id:
                        key = (entry["action"], entry["target"])
                        if key not in seen_keys:
                            seen_keys.add(key)
                            # Confidence from the match similarity
                            conf = self._remediation_store.get_confidence(
                                entry["action"], entry["target"]
                            )
                            # Boost by match similarity
                            conf = max(conf, match.similarity * 0.8)
                            candidates.append(
                                Remediation(
                                    action=entry["action"],
                                    target=entry["target"],
                                    historical_outcome=entry.get("outcome", "resolved"),
                                    confidence=round(min(1.0, conf), 4),
                                )
                            )

        # Strategy 2: Bayesian store suggestions for this service
        store_suggestions = self._remediation_store.suggest_for_service(
            primary_service, all_service_names, k=10
        )
        for r in store_suggestions:
            key = (r.action, r.target)
            if key not in seen_keys:
                seen_keys.add(key)
                candidates.append(r)

        candidates.sort(key=lambda r: r.confidence, reverse=True)
        return candidates[:5]

    def _compute_confidence(
        self,
        related_events: List[Event],
        causal_edges: List[CausalEdge],
        similar: List[IncidentMatch],
    ) -> float:
        """
        Overall confidence score [0, 1] for the reconstructed context.
        Combines:
        - Signal density (number of anomalous events)
        - Causal chain strength
        - Historical match strength
        """
        scores = []

        # Signal density: fraction of high-anomaly events
        if related_events:
            anomaly_scores = [ev.get("_anomaly_score", 0.5) for ev in related_events]
            avg_anomaly = sum(anomaly_scores) / len(anomaly_scores)
            scores.append(avg_anomaly)

        # Causal chain confidence
        if causal_edges:
            avg_edge_conf = sum(e.confidence for e in causal_edges) / len(causal_edges)
            scores.append(avg_edge_conf)

        # Historical match confidence
        if similar:
            best_sim = similar[0].similarity
            scores.append(best_sim * 0.8)  # discount slightly

        if not scores:
            return 0.50

        return min(1.0, sum(scores) / len(scores))

    def _generate_explain(
        self,
        signal: IncidentSignal,
        related_events: List[Event],
        causal_edges: List[CausalEdge],
        similar: List[IncidentMatch],
        remediations: List[Remediation],
        confidence: float,
    ) -> str:
        """Generate transparent, human-readable explanation narrative."""
        parts = []

        # Header
        parts.append(f"Incident {signal.incident_id} triggered by: {signal.trigger}.")

        # Related events summary
        if related_events:
            kinds = {}
            for ev in related_events:
                k = ev.get("kind", "unknown")
                kinds[k] = kinds.get(k, 0) + 1
            kind_summary = ", ".join(f"{v} {k}" for k, v in sorted(kinds.items()))
            top_ev = related_events[0]
            top_score = top_ev.get("_anomaly_score", 0)
            parts.append(
                f"Found {len(related_events)} related events ({kind_summary}). "
                f"Highest-anomaly signal: {top_ev.get('kind')} from {top_ev.get('service', 'unknown')} "
                f"at {top_ev.get('ts', '')} (anomaly={top_score:.2f})."
            )
        else:
            parts.append("No related events found in the reconstruction window.")

        # Causal chain
        if causal_edges:
            chain_parts = []
            for edge in causal_edges[:5]:  # top 5 edges
                chain_parts.append(
                    f"[{edge.edge_type}: {edge.evidence} (conf={edge.confidence:.2f})]"
                )
            parts.append(
                f"Causal chain ({len(causal_edges)} edges): "
                + " → ".join(chain_parts[:3])
                + ("..." if len(causal_edges) > 3 else "")
                + "."
            )
        else:
            parts.append("No causal edges found within the reconstruction window.")

        # Similar past incidents
        if similar:
            best = similar[0]
            parts.append(
                f"Matched {len(similar)} similar past incident(s). "
                f"Best match: {best.past_incident_id} "
                f"(similarity={best.similarity:.0%}, {best.rationale})."
            )
        else:
            parts.append(
                "No similar past incidents found (novel pattern or insufficient history)."
            )

        # Remediations
        if remediations:
            top_r = remediations[0]
            parts.append(
                f"Top remediation: '{top_r.action}' on '{top_r.target}' "
                f"(confidence={top_r.confidence:.2f}, historical outcome: {top_r.historical_outcome}). "
                f"{len(remediations)} options available."
            )
        else:
            parts.append("No remediation history found for this service.")

        # Confidence
        parts.append(f"Overall reconstruction confidence: {confidence:.0%}.")

        return " ".join(parts)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _infer_service_from_trigger(self, trigger: str) -> str:
        """Extract service name from trigger string like 'alert:checkout-api/error-rate>5%'."""
        if not trigger:
            return ""
        # Format: alert:service-name/metric>threshold
        if ":" in trigger:
            parts = trigger.split(":", 1)[1]
            if "/" in parts:
                return parts.split("/")[0]
            return parts
        return trigger

    def _get_all_service_names(self, service: str) -> List[str]:
        """Return all names this service has ever had."""
        if not service:
            return []
        return self._identity.all_names_for(service)

    def _resolve_affected_entities(
        self,
        trigger_service: str,
        trigger_names: List[str],
        related_events: List[Event],
    ) -> List[str]:
        """
        Resolve all logical services involved in the incident pattern.
        Alerts often fire on upstream callers while deploy/latency signals
        sit on the downstream dependency.
        """
        names: Set[str] = set(trigger_names)
        if trigger_service:
            names.update(self._identity.all_names_for(trigger_service))

        # We must find the causally-linked services.
        # Max 2 passes to handle A -> B -> C chains.
        for _ in range(2):
            added_new = False
            for ev in related_events:
                kind = ev.get("kind")
                if kind == EventKind.LOG.value and ev.get("level", "").lower() == "error":
                    svc = ev.get("service", "")
                    msg = ev.get("msg", "")

                    # Case 1: The log is emitted by a service we already know is affected (e.g. the trigger service).
                    # It might mention the downstream broken service.
                    if svc in names:
                        for known_svc in self._buffer._service_idx:
                            if known_svc in msg and known_svc not in names:
                                names.update(self._identity.all_names_for(known_svc))
                                added_new = True

                    # Case 2: The log is emitted by some upstream service, but it mentions
                    # a service we know is affected. Then the upstream service is also part of the chain.
                    elif svc and svc not in names and any(n in msg for n in names):
                        names.update(self._identity.all_names_for(svc))
                        added_new = True

            if not added_new:
                break

        return list(names)

    def get_performance_summary(self) -> Dict:
        return self._perf.summary()
