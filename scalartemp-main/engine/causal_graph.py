"""
causal_graph.py — Temporal Directed Acyclic Graph of causal relationships.

Properties:
- Cause timestamp ≤ Effect timestamp (always enforced)
- Edges stored as CausalEdge dataclasses
- Probabilistic: confidence + contradiction tracking
- Incremental: edges added at ingest time, never rebuilt
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

from engine.schema import CausalEdge


class CausalGraph:
    """
    Append-only causal graph.

    Edges are indexed by cause_idx and effect_idx for fast window queries.
    Temporal ordering is enforced on add.
    """

    def __init__(self):
        self._edges: List[CausalEdge] = []
        # cause_idx → list of edge positions in self._edges
        self._cause_idx: Dict[int, List[int]] = defaultdict(list)
        # effect_idx → list of edge positions
        self._effect_idx: Dict[int, List[int]] = defaultdict(list)
        # (cause_idx, effect_idx, edge_type) → edge position (for dedup/updates)
        self._edge_key_map: Dict[Tuple[int, int, str], int] = {}
        # Node degree tracking for entropy penalty
        self._node_degree: Dict[int, int] = defaultdict(int)

    def add_edge(self, edge: CausalEdge) -> bool:
        """
        Add or update a causal edge.
        If the same (cause, effect, type) triple already exists, update confidence.
        Returns True if edge was added/updated.
        """
        if edge.cause_idx >= edge.effect_idx:
            # Enforce temporal ordering by buffer position
            # (higher index = later in stream = later event)
            return False

        key = (edge.cause_idx, edge.effect_idx, edge.edge_type)
        if key in self._edge_key_map:
            # Update existing edge — increment sample count, recompute confidence
            pos = self._edge_key_map[key]
            existing = self._edges[pos]
            existing.sample_count += 1
            # Bayesian-ish update: running average
            n = existing.sample_count
            existing.confidence = (existing.confidence * (n - 1) + edge.confidence) / n
            return True

        pos = len(self._edges)
        self._edges.append(edge)
        self._cause_idx[edge.cause_idx].append(pos)
        self._effect_idx[edge.effect_idx].append(pos)
        self._edge_key_map[key] = pos

        # Track degree for entropy-based graph damping
        self._node_degree[edge.cause_idx] += 1
        self._node_degree[edge.effect_idx] += 1
        return True

    def get_edges_in_window(
        self,
        event_indices: Set[int],
        min_confidence: float = 0.0,
    ) -> List[CausalEdge]:
        """
        Return all edges where BOTH cause and effect are within the given index set.
        Optionally filter by minimum confidence.
        """
        import math

        results = []
        seen = set()
        for idx in event_indices:
            degree = self._node_degree.get(idx, 1)
            damping = max(1.0, math.log10(degree + 1))
            # Edges where this event is the cause
            for pos in self._cause_idx.get(idx, []):
                if pos in seen:
                    continue
                edge = self._edges[pos]
                adj_conf = min(1.0, edge.confidence / damping)
                if edge.effect_idx in event_indices and adj_conf >= min_confidence:
                    adjusted_edge = CausalEdge(
                        cause_idx=edge.cause_idx,
                        effect_idx=edge.effect_idx,
                        edge_type=edge.edge_type,
                        confidence=adj_conf,
                        evidence=edge.evidence,
                        sample_count=edge.sample_count,
                    )
                    results.append(adjusted_edge)
                    seen.add(pos)
        return results

    def get_edges_from(self, cause_idx: int) -> List[CausalEdge]:
        """All edges emanating from cause_idx."""
        return [self._edges[p] for p in self._cause_idx.get(cause_idx, [])]

    def get_edges_to(self, effect_idx: int) -> List[CausalEdge]:
        """All edges pointing to effect_idx."""
        return [self._edges[p] for p in self._effect_idx.get(effect_idx, [])]

    def topological_sort(self, edges: List[CausalEdge]) -> List[CausalEdge]:
        """Sort edges by (cause_idx, effect_idx) — already temporal by construction."""
        return sorted(edges, key=lambda e: (e.cause_idx, e.effect_idx))

    def validate_temporal_ordering(self) -> bool:
        """Check that all edges respect cause_idx < effect_idx."""
        return all(e.cause_idx < e.effect_idx for e in self._edges)

    def get_causal_chain(
        self,
        root_idx: int,
        max_depth: int = 10,
    ) -> Tuple[List[CausalEdge], Dict[str, Any]]:
        """
        BFS forward from root_idx to find the causal chain.
        Returns (edges in topological order, propagation_signature_dict).
        """
        import math

        visited = set()
        queue = [(root_idx, 0)]  # (idx, depth)
        result_edges = []
        max_fanout_depth = 0
        total_traversals = 0

        while queue:
            idx, current_depth = queue.pop(0)
            if idx in visited or current_depth >= max_depth:
                continue
            visited.add(idx)
            max_fanout_depth = max(max_fanout_depth, current_depth)

            # Entropy edge filtering: damp confidence by out-degree of cause
            degree = self._node_degree.get(idx, 1)
            damping = max(1.0, math.log10(degree + 1))

            for edge in self.get_edges_from(idx):
                total_traversals += 1
                # Apply entropy penalty dynamically on retrieval
                adjusted_edge = CausalEdge(
                    cause_idx=edge.cause_idx,
                    effect_idx=edge.effect_idx,
                    edge_type=edge.edge_type,
                    confidence=min(1.0, edge.confidence / damping),
                    evidence=edge.evidence,
                    sample_count=edge.sample_count,
                )
                if adjusted_edge.confidence >= 0.15:  # drop very weak edges
                    result_edges.append(adjusted_edge)
                    queue.append((edge.effect_idx, current_depth + 1))

        propagation_signature = {
            "fanout_depth": max_fanout_depth,
            "total_traversals": total_traversals,
        }
        return self.topological_sort(result_edges), propagation_signature

    @property
    def edge_count(self) -> int:
        return len(self._edges)

    def summary(self) -> Dict:
        type_counts: Dict[str, int] = defaultdict(int)
        for e in self._edges:
            type_counts[e.edge_type] += 1
        return {
            "total_edges": self.edge_count,
            "by_type": dict(type_counts),
            "avg_confidence": (
                sum(e.confidence for e in self._edges) / len(self._edges)
                if self._edges
                else 0.0
            ),
        }
