# Persistent Context Engine — Autonomous SRE

A substrate for operational memory to preserve causal chains, handle topology drift, and adaptively reconstruct context for distributed systems.

## Features

- **Topology-Invariant Fingerprinting**: Generates behavioral signatures that survive service renames and dependency shifts.
- **Probabilistic Temporal DAG**: Ingests disparate signals (logs, metrics, traces, deploys) into an append-only causal graph.
- **Fuzzy Incident Matching**: Finds similar past incidents using Levenshtein distance modified with Subsequence Matching on historical behavioral fingerprints.
- **Bayesian Remediation Engine**: Recommends historical fixes with confidence scores reflecting past successes.
- **Fast / Deep Modes**: Reconstructs contexts in under 2 seconds for p95, with wider search capabilities in "deep" mode.

## Installation

You can install this engine directly via pip:

```bash
cd scalarhack
pip install .
```

This will install the internal components and register the CLI command `context-engine`.

## SDK Usage

```python
from engine import Engine

# Initialize the engine
engine = Engine()

# 1. Ingest telemetry streams (logs, metrics, deploys, traces, topology)
telemetry_events = [
    {"ts": "2026-05-10T14:21:30Z", "kind": "deploy", "service": "payments-svc", "version": "v2.14.0"},
    {"ts": "2026-05-10T14:32:11Z", "kind": "incident_signal", "incident_id": "INC-714", "trigger": "alert:checkout-api/error-rate>5%"},
]
engine.ingest(telemetry_events)

# 2. Reconstruct context from an incident signal
signal = {
    "ts": "2026-05-10T14:32:11Z", 
    "kind": "incident_signal", 
    "incident_id": "INC-714", 
    "trigger": "alert:checkout-api/error-rate>5%"
}

# mode can be 'fast' or 'deep'
context = engine.reconstruct_context(signal, mode="fast")

print(f"Confidence: {context['confidence']:.0%}")
print(f"Explain: {context['explain']}")
print(f"Suggested Remedies: {context['suggested_remediations']}")
```

## CLI Usage

The SDK ships directly with a command-line utility.

### Status

Check the internal state arrays:
```bash
context-engine status
```

### Ingestion

Ingest JSONL log telemetry from a file:
```bash
context-engine ingest /path/to/telemetry.jsonl
```

### Query Context

Evaluate an incident signal directly via CLI payload:
```bash
context-engine query '{"ts": "2026-05-10T14:32:11Z", "kind": "incident_signal", "incident_id": "INC-714", "trigger": "alert:checkout-api/error-rate>5%"}'
```

### Replay & Solve

Simulate full state ingestion and automatically reconstruct context whenever an `incident_signal` occurs:
```bash
context-engine replay /path/to/trace.jsonl
```

## Internal Architecture

The components inside this engine are separated into:
- `telemetry_buffer.py`: Circular thread-safe buffer for state ingestion.
- `causal_graph.py`: Append-only, probabilistic temporal DAG.
- `rules_engine.py`: Maps edges between objects dynamically matching correlations.
- `memory_substrate.py`: State and statistics keeping tracking identities.
- `context_compiler.py`: Evaluator to process reconstruction rules.
- `incident_fingerprinter.py`: Pattern classification hashing.
- `remediation_store.py`: Bayesian state matching for operational outcome feedback.
