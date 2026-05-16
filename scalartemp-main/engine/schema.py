"""
schema.py — Shared data contracts for the Persistent Context Engine.
IMMUTABLE after initialization. All modules import from here.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class EventKind(str, Enum):
    DEPLOY = "deploy"
    LOG = "log"
    METRIC = "metric"
    TRACE = "trace"
    TOPOLOGY = "topology"
    INCIDENT_SIGNAL = "incident_signal"
    REMEDIATION = "remediation"


class RemediationOutcome(str, Enum):
    RESOLVED = "resolved"
    PARTIAL = "partial"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# Raw event dict type alias (as received from JSONL)
# ---------------------------------------------------------------------------
Event = Dict[str, Any]


@dataclass
class CausalEdge:
    cause_idx: int  # Index of cause event in buffer
    effect_idx: int  # Index of effect event in buffer
    edge_type: str  # "temporal" | "correlation" | "deployment" | "behavioral"
    evidence: str  # Human-readable explanation
    confidence: float  # 0.0 to 1.0
    temporal_gap_ms: int = 0  # Milliseconds between cause and effect
    sample_count: int = 1  # Observations supporting this edge
    contradictions: int = 0  # Failed validations

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cause_idx": self.cause_idx,
            "effect_idx": self.effect_idx,
            "edge_type": self.edge_type,
            "evidence": self.evidence,
            "confidence": round(self.confidence, 4),
            "temporal_gap_ms": self.temporal_gap_ms,
            "sample_count": self.sample_count,
            "contradictions": self.contradictions,
        }


@dataclass
class IncidentMatch:
    past_incident_id: str
    similarity: float  # Levenshtein ratio 0..1
    rationale: str  # Why this matched
    past_fingerprint: str = ""
    current_fingerprint: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.past_incident_id,
            "past_incident_id": self.past_incident_id,
            "similarity": round(self.similarity, 4),
            "rationale": self.rationale,
            "past_fingerprint": self.past_fingerprint,
            "current_fingerprint": self.current_fingerprint,
        }


@dataclass
class Remediation:
    action: str
    target: str
    historical_outcome: str  # "resolved" | "partial" | "failed"
    confidence: float  # Bayesian posterior

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "target": self.target,
            "historical_outcome": self.historical_outcome,
            "confidence": round(self.confidence, 4),
        }


@dataclass
class IncidentSignal:
    incident_id: str
    ts: str
    trigger: str
    service: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass
class Context:
    related_events: List[Event]  # ordered, deduped, with provenance
    causal_chain: List[CausalEdge]  # (cause_id, effect_id, evidence, confidence)
    similar_past_incidents: List[IncidentMatch]  # topology-invariant matches
    suggested_remediations: List[
        Remediation
    ]  # (action, target, historical_outcome, confidence)
    confidence: float  # overall 0..1
    explain: str  # human-readable narrative

    def to_dict(self) -> Dict[str, Any]:
        return {
            "related_events": self.related_events,
            "causal_chain": [e.to_dict() for e in self.causal_chain],
            "similar_past_incidents": [
                m.to_dict() for m in self.similar_past_incidents
            ],
            "suggested_remediations": [
                r.to_dict() for r in self.suggested_remediations
            ],
            "confidence": round(self.confidence, 4),
            "explain": self.explain,
        }
