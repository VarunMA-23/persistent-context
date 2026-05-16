"""
remediation_store.py — Bayesian remediation outcome tracking and learning.

Tracks every (action, target) pair with outcome counts.
Computes confidence using Bayesian posteriors.
Improves suggestions over time as outcomes accumulate.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from engine.schema import Remediation


class RemediationStore:
    """
    Persistent store of remediation actions and their outcomes.

    Uses Bayesian confidence:
    - Prior: P(success) = 0.5 (unknown action)
    - Posterior: success_rate × sample_multiplier
    - sample_multiplier = min(1.0, sample_count / 10)
    - With 1 sample: confidence ≈ 0.1×success_rate
    - With 10+ samples: confidence = success_rate
    """

    def __init__(self):
        # (action, target) → outcome counts
        self._counts: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(
            lambda: {"resolved": 0, "partial": 0, "failed": 0, "total": 0}
        )
        # Ordered log for temporal analysis
        self._log: List[Dict] = []
        self._incident_ids: set[str] = set()

    def record(self, action: str, target: str, outcome: str, incident_id: str = ""):
        """Record a remediation outcome."""
        key = (action, target)
        counts = self._counts[key]

        outcome_norm = outcome.lower().strip()
        if outcome_norm in ("resolved", "partial", "failed"):
            counts[outcome_norm] += 1
        else:
            counts["partial"] += 1  # unknown → treat as partial

        counts["total"] += 1

        self._log.append(
            {
                "action": action,
                "target": target,
                "outcome": outcome_norm,
                "incident_id": incident_id,
            }
        )
        if incident_id:
            self._incident_ids.add(incident_id)

    def has_incident(self, incident_id: str) -> bool:
        """True if a remediation was recorded for this incident."""
        return incident_id in self._incident_ids

    def get_success_rate(self, action: str, target: str) -> float:
        """Return P(resolved) for this (action, target) pair."""
        key = (action, target)
        if key not in self._counts:
            return 0.5  # prior
        counts = self._counts[key]
        total = counts["total"]
        if total == 0:
            return 0.5
        return counts["resolved"] / total

    def get_confidence(self, action: str, target: str) -> float:
        """
        Bayesian confidence: success_rate × min(1.0, sample_count / 10).
        Low samples → low confidence regardless of success rate.
        """
        key = (action, target)
        if key not in self._counts:
            return 0.05  # unknown action, very low confidence
        counts = self._counts[key]
        total = counts["total"]
        if total == 0:
            return 0.05
        success_rate = counts["resolved"] / total
        sample_multiplier = min(1.0, total / 10.0)
        return success_rate * sample_multiplier

    def get_best_outcome(self, action: str, target: str) -> str:
        """Return the most common outcome for this action."""
        key = (action, target)
        if key not in self._counts:
            return "resolved"
        counts = self._counts[key]
        best = max(["resolved", "partial", "failed"], key=lambda k: counts[k])
        return best

    def suggest_top_k(
        self, k: int = 5, service_filter: Optional[str] = None
    ) -> List[Remediation]:
        """
        Return top-k remediations ranked by Bayesian confidence.
        Optionally filter by target service.
        """
        candidates = []
        for (action, target), counts in self._counts.items():
            if service_filter and target != service_filter:
                # Still include if no exact filter match — fall back to all
                pass
            conf = self.get_confidence(action, target)
            outcome = self.get_best_outcome(action, target)
            candidates.append(
                Remediation(
                    action=action,
                    target=target,
                    historical_outcome=outcome,
                    confidence=conf,
                )
            )

        # Sort by confidence descending
        candidates.sort(key=lambda r: r.confidence, reverse=True)

        # If service_filter, prioritize matching targets but include others
        if service_filter:
            matching = [r for r in candidates if r.target == service_filter]
            others = [r for r in candidates if r.target != service_filter]
            ordered = matching + others
        else:
            ordered = candidates

        return ordered[:k]

    def suggest_for_service(
        self, service: str, all_names: List[str], k: int = 5
    ) -> List[Remediation]:
        """
        Suggest remediations for any name this service has had.
        Topology-invariant: checks all historical names.
        """
        candidates = []
        seen_keys = set()
        for name in all_names:
            for r in self.suggest_top_k(k=50, service_filter=name):
                key = (r.action, r.target)
                if key not in seen_keys:
                    seen_keys.add(key)
                    candidates.append(r)

        # Also include global top suggestions as fallback
        for r in self.suggest_top_k(k=50):
            key = (r.action, r.target)
            if key not in seen_keys:
                seen_keys.add(key)
                candidates.append(r)

        candidates.sort(key=lambda r: r.confidence, reverse=True)
        return candidates[:k]

    @property
    def total_records(self) -> int:
        return len(self._log)

    def summary(self) -> Dict:
        total_actions = len(self._counts)
        overall_success = 0
        total_total = 0
        for counts in self._counts.values():
            overall_success += counts["resolved"]
            total_total += counts["total"]
        return {
            "distinct_action_target_pairs": total_actions,
            "total_outcomes_recorded": total_total,
            "global_success_rate": (
                overall_success / total_total if total_total > 0 else 0.0
            ),
        }
