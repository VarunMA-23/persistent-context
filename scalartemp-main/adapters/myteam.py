"""
Anvil Benchmark Adapter — thin shim over Persistent Context Engine.

Conforms to the Adapter base class expected by the bench harness
(self_check.py / run.py).

Usage:
    python self_check.py --adapter adapters.myteam:Engine --quick
    python run.py --adapter adapters.myteam:Engine --mode fast --seeds 42
"""

from __future__ import annotations

import os
import sys

# Ensure engine package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Any, Dict, Iterable

from engine import Engine as _Engine


class Engine:
    """Adapter shim bridging the Anvil benchmark harness to our engine."""

    def __init__(self):
        self._engine = _Engine()

    def ingest(self, events: Iterable[Dict[str, Any]]) -> None:
        """Ingest a batch of telemetry events."""
        self._engine.ingest(events)

    def reconstruct_context(
        self,
        signal: Dict[str, Any],
        mode: str = "fast",
    ) -> Dict[str, Any]:
        """Reconstruct investigation context for an incident signal."""
        return self._engine.reconstruct_context(signal, mode=mode)

    def close(self) -> None:
        """Graceful shutdown."""
        self._engine.close()
