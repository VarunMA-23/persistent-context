"""
Benchmark adapter for the Persistent Context Engine (scalartemp-main).
"""
from __future__ import annotations

import os
import sys
from typing import Any, Dict, Iterable, Literal

# Path to scalartemp-main
_engine_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "scalartemp-main"))
if _engine_root not in sys.path:
    sys.path.insert(0, _engine_root)

from adapter import Adapter
from engine import Engine as PersistentContextEngine
from schema import Context, Event, IncidentSignal

class Engine(Adapter):
    """Thin shim over scalartemp-main Persistent Context Engine."""

    def __init__(self) -> None:
        self._engine = PersistentContextEngine()

    def ingest(self, events: Iterable[Event]) -> None:
        self._engine.ingest(events)

    def reconstruct_context(
        self,
        signal: IncidentSignal,
        mode: Literal["fast", "deep"] = "fast",
    ) -> Context:
        return self._engine.reconstruct_context(dict(signal), mode=mode)

    def close(self) -> None:
        self._engine.close()
