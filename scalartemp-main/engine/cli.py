#!/usr/bin/env python3
"""
cli.py - Command Line Interface for the Persistent Context Engine.
"""

import argparse
import json
import sys
from typing import Any, Dict, List

from engine import Engine


def cmd_ingest(engine: Engine, args):
    """Ingest JSONL telemetry from a file."""
    events: List[Dict[str, Any]] = []
    lines_read = 0
    try:
        with open(args.file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                events.append(json.loads(line))
                lines_read += 1
    except FileNotFoundError:
        print(f"Error: File '{args.file}' not found.", file=sys.stderr)
        return 1

    engine.ingest(events)
    print(f"Successfully ingested {lines_read} events.")
    return 0


def cmd_query(engine: Engine, args):
    """Query context for an incident signal."""
    # Convert JSON string to raw dictionary
    try:
        signal = json.loads(args.signal)
    except json.JSONDecodeError as e:
        print(f"Error parse signal JSON: {e}", file=sys.stderr)
        return 1

    ctx = engine.reconstruct_context(signal, mode=args.mode)
    print(json.dumps(ctx, indent=2))
    return 0


def cmd_status(engine: Engine, args):
    """Display internal state metrics."""
    print("=== Engine Status ===")
    print(f"Telemetry Buffer: {engine._buffer._count} events appended")
    print(f"Causal Graph:     {engine._graph.edge_count} causal edges extracted")
    print(f"Service Identity: {len(engine._identity._name_to_id)} tracked entities")
    print(
        f"Past Incidents:   {engine._family_registry.size} cached behavior fingerprints"
    )
    return 0


def cmd_replay(engine: Engine, args):
    """Replay telemetry and report on every incident_signal found."""
    lines_read = 0
    incident_count = 0
    events = []

    print(f"Reading events from {args.file}...")
    try:
        with open(args.file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                events.append(json.loads(line))
                lines_read += 1
    except FileNotFoundError:
        print(f"Error: File '{args.file}' not found.", file=sys.stderr)
        return 1

    print(f"Evaluating {lines_read} events...\n")
    for event in events:
        engine.ingest([event])
        if event.get("kind") == "incident_signal":
            incident_count += 1
            print(f"--- Incident Detected: {event.get('incident_id')} ---")
            ctx = engine.reconstruct_context(event, mode="fast")
            print(f"Confidence: {ctx.get('confidence'):.0%}")
            print(f"Explanation: {ctx.get('explain')}\n")

    print(f"Replay complete. Found and reconstructed {incident_count} incidents.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Persistent Context Engine CLI")
    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Subcommands"
    )

    # ingest
    cmd_ing = subparsers.add_parser("ingest", help="Ingest JSONL telemetry")
    cmd_ing.add_argument("file", help="Path to JSONL file")

    # query
    cmd_qry = subparsers.add_parser("query", help="Reconstruct context for a signal")
    cmd_qry.add_argument("signal", help="JSON string representing the IncidentSignal")
    cmd_qry.add_argument(
        "--mode", choices=["fast", "deep"], default="fast", help="Reconstruction mode"
    )

    # status
    subparsers.add_parser("status", help="Show engine status")

    # replay
    cmd_rep = subparsers.add_parser(
        "replay", help="Replay a trace JSONL resolving every incident found"
    )
    cmd_rep.add_argument("file", help="Path to JSONL trace file")

    args = parser.parse_args()

    engine = Engine()

    # Dispatch
    if args.command == "ingest":
        sys.exit(cmd_ingest(engine, args))
    elif args.command == "query":
        sys.exit(cmd_query(engine, args))
    elif args.command == "status":
        sys.exit(cmd_status(engine, args))
    elif args.command == "replay":
        sys.exit(cmd_replay(engine, args))


if __name__ == "__main__":
    main()
