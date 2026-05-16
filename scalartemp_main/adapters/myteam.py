"""
Adapter entry for `scalartemp_main.adapters.myteam:Engine` with PYTHONPATH=repository root.

Resolves `scalartemp-main/` (hyphenated directory) and delegates to the PCE `engine` package.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, Iterable

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scalartemp-main"))
if _root not in sys.path:
    sys.path.insert(0, _root)

from engine import Engine as _Engine


class Engine:
    """Thin shim for Anvil harness → Persistent Context Engine."""

    def __init__(self) -> None:
        self._engine = _Engine()

    def ingest(self, events: Iterable[Dict[str, Any]]) -> None:
        self._engine.ingest(events)

    def reconstruct_context(
        self,
        signal: Dict[str, Any],
        mode: str = "fast",
    ) -> Dict[str, Any]:
        return self._engine.reconstruct_context(signal, mode=mode)

    def close(self) -> None:
        self._engine.close()
