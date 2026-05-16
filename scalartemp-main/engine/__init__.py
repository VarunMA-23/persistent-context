"""
engine — Persistent Context Engine for Autonomous SRE.

This is the main adapter that wires all internal components together and
exposes the two operations required by the Anvil benchmark SDK:

    engine.ingest(events)
    engine.reconstruct_context(signal, mode="fast") -> Context (dict)

Architecture:
    TelemetryBuffer ──► RulesEngine ──► CausalGraph
         │                   │               │
         │              ServiceIdentity      │
         │              BaselineStats        │
         │              IncidentFamilyRegistry
         │              RemediationStore     │
         └──────────────────────────────────►│
                                   ContextCompiler
"""

from __future__ import annotations

from typing import Any, Dict, Iterable

from engine.causal_graph import CausalGraph
from engine.context_compiler import ContextCompiler
from engine.memory_substrate import (
    BaselineStats,
    IncidentFamilyRegistry,
    ServiceIdentity,
)
from engine.remediation_store import RemediationStore
from engine.rules_engine import RulesEngine
from engine.schema import Context, Event, EventKind, IncidentSignal
from engine.telemetry_buffer import TelemetryBuffer


class Engine:
    """
    Persistent Context Engine for Autonomous SRE.

    Conforms to the Anvil SDK interface:
        ingest(events: Iterable[Event]) -> None
        reconstruct_context(signal, mode) -> Context (dict)
        close() -> None

    Internal components are stateful and wire together the processing pipeline.
    """

    def __init__(self):
        # --- Memory substrate ---
        self._buffer = TelemetryBuffer(max_events=500_000)
        self._graph = CausalGraph()
        self._identity = ServiceIdentity()
        self._baseline = BaselineStats(window=1000)
        self._family_registry = IncidentFamilyRegistry(cache_size=200)
        self._remediation_store = RemediationStore()

        # --- Processing pipeline ---
        self._rules = RulesEngine(
            buffer=self._buffer,
            graph=self._graph,
            identity=self._identity,
            baseline=self._baseline,
            family_registry=self._family_registry,
        )
        self._compiler = ContextCompiler(
            buffer=self._buffer,
            graph=self._graph,
            identity=self._identity,
            baseline=self._baseline,
            family_registry=self._family_registry,
            remediation_store=self._remediation_store,
        )

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def ingest(self, events: Iterable[Event]) -> None:
        """
        Ingest a stream of telemetry events.

        Each event is:
          1. Appended to the circular buffer (provenance preserved)
          2. Processed by the rules engine (causal edges, fingerprints, baselines)
          3. If it's a remediation event, recorded in the remediation store
        """
        for raw_event in events:
            idx = self._buffer.append(raw_event)
            if idx < 0:
                continue  # rejected (invalid kind)

            # Retrieve the stamped copy (with _index and _ingested_at)
            stamped = self._buffer.get_by_index(idx)
            if stamped is None:
                continue

            # Fire rules (temporal proximity, correlation, deployment adjacency,
            # incident family extraction)
            self._rules.process(stamped)

            # Record remediation outcomes for Bayesian learning
            if stamped.get("kind") == EventKind.REMEDIATION.value:
                self._remediation_store.record(
                    action=stamped.get("action", "unknown"),
                    target=stamped.get("target", "unknown"),
                    outcome=stamped.get("outcome", "partial"),
                    incident_id=stamped.get("incident_id", ""),
                )

    # ------------------------------------------------------------------
    # Reconstruct Context
    # ------------------------------------------------------------------

    def reconstruct_context(
        self,
        signal_dict: Dict[str, Any],
        mode: str = "fast",
    ) -> Dict[str, Any]:
        """
        Reconstruct investigation context for an incident signal.

        Args:
            signal_dict: Raw incident signal dict with keys:
                incident_id, ts, trigger, and optionally service.
            mode: "fast" (p95 < 2s) or "deep" (p95 < 6s).

        Returns:
            Context as a plain dict matching the SDK schema.
        """
        # Build IncidentSignal from raw dict
        incident_signal = IncidentSignal(
            incident_id=signal_dict.get("incident_id", ""),
            ts=signal_dict.get("ts", ""),
            trigger=signal_dict.get("trigger", ""),
            service=signal_dict.get("service"),
            raw=signal_dict,
        )

        # Delegate to the context compiler
        ctx: Context = self._compiler.reconstruct_context(incident_signal, mode=mode)

        # Convert to plain dict for SDK compatibility
        return ctx.to_dict()

    def close(self):
        """Graceful shutdown. No-op for in-memory engine."""
        pass


__all__ = ["Engine"]
