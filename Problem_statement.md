Persistent Context Engine for autonomous SRE.
Existing observability stores telemetry. It does not preserve operational reasoning. Every incident forces engineers to rebuild causal chains, correlate fragmented signals, and rediscover behaviors the system has already encountered. Build the substrate that ends that loop.

Problem
Distributed production environments evolve continuously: services are renamed, dependencies shift, deployments mutate behavior, operational patterns drift, topology structures change. Traditional observability is optimized for visualization, querying, alerting, and short-horizon analysis — not for preserving long-term operational understanding. As complexity rises, recurring incidents reappear under different telemetry signatures, modified topologies, renamed services, or altered dependency structures.

Most AI-assisted observability approaches lean on semantic retrieval, embedding similarity, keyword search, static graph correlation, or incident retrieval pipelines. All degrade under topology drift, temporal evolution, noisy telemetry, partial observability, and changing infrastructure semantics. The challenge is therefore a systems problem.

Core Question
How can operational telemetry be transformed into persistent memory capable of adaptive reasoning across evolving distributed environments?
System Expectations
The Persistent Context Engine functions as an operational memory substrate. Instead of storing telemetry as isolated searchable records, it should continuously synthesize evolving contextual relationships directly from operational behavior — temporal reasoning, arbitrary relationship formation, adaptive contextual reconstruction, behavioral abstraction, evolving operational associations, and topology-independent reasoning.

Relationships may emerge from correlated degradation, deployment adjacency, repeated co-occurrence, remediation sequences, inferred causality, operator workflows, or latent operational patterns. No fixed schema or rigid ontology is mandated. Dynamic edge formation and continuously evolving topology structures are first-class.

During incident analysis, the engine reconstructs investigation context dynamically, traverses historical operational memory, surfaces historically validated remediation pathways, identifies recurring behavioral patterns, and synthesizes signal-dense operational understanding. The expected output is not a search result. It is reconstructed operational context.

Architectural Scope
Any architecture, infrastructure stack, or storage model. Possible directions include —

Graph-based systems
Temporal memory architectures
Probabilistic structures
Distributed streaming systems
Contextual synthesis pipelines
Reinforcement-based memory systems
Hybrid indexing strategies
Custom operational memory substrates
Core Capabilities
01 · Operational Ingestion
Continuously ingest large-scale telemetry streams. Preserve provenance and temporal ordering. Support replayability and historical operational continuity.
02 · Dynamic Relationship Synthesis
Construct relationships without predefined schemas. Support evolving edge semantics. Preserve probabilistic and contradictory signals where necessary. Synthesize associations adaptively.
03 · Long-Horizon Operational Memory
Preserve contextual understanding across infrastructure drift. Recognize behavioral equivalence across changing environments. Support reinforcement and decay. Maintain persistence over time.
04 · Adaptive Context Compilation
At incident time, reconstruct investigation context dynamically. Prioritize high-signal understanding. Surface relevant historical behaviors. Compile context adaptively from evolving memory.
05 · Incident Shape Recognition
Identify recurring operational behaviors independent of topology. Support approximate behavioral matching. Recognize evolving failure signatures. Detect operational equivalence despite telemetry drift.
06 · Continuous Learning
Reinforce successful remediation pathways. Evolve contextual salience through operational feedback. Improve reconstruction quality over time.
07 · Scalability
Operate under high-ingestion telemetry workloads. Maintain efficient contextual synthesis. Support low-latency operational reconstruction.
Benchmark & Evaluation
The benchmark harness is publicly available in the linked repository and runs locally on participants' machines — pure Python, stdlib only, no external dependencies. Held-out adversarial scenarios (L3) at higher parameter values are revealed only at final evaluation. The benchmark simulates distributed production systems under evolving failure conditions. Datasets include deployment sequences, infrastructure mutations, incident timelines, remediation histories, telemetry drift, topology evolution, and changing dependency structures.

Area	Description
Incident Recall	Identify historically similar incidents
Context Quality	Relevance and signal density of synthesized context
Retrieval Accuracy	Correctness of surfaced operational memory
Pattern Recognition	Detection of recurring operational behaviors
Temporal Reasoning	Handling evolving infrastructure states
Adaptability	Robustness under topology drift
Latency	Runtime context-synthesis performance
Scalability	Throughput under large ingestion
Explainability	Inspectable reasoning pathways
Memory Evolution	Improvement through operational feedback
Expected Outcome
Persistent operational memory formation. Adaptive contextual reconstruction. Recurring incident recognition. Topology-independent behavioral understanding. Evolving operational reasoning. Measurable improvement through operational feedback loops.

North Star
Not a dashboard. Not a log viewer. Not a retrieval wrapper. An operational memory engine.
Annex A — Operational Contract
What to ship, how to ship it, how it will be judged.
This section is binding. The manifesto above is open to interpretation; the annex is not. The public benchmark harness in the linked repository adheres to these shapes. Held-out adversarial scenarios (L3) at higher parameters are revealed only at final evaluation — your engine must conform to both.
Input · Telemetry Shape
The engine consumes a stream of timestamped events in newline-delimited JSON. Six event kinds are guaranteed; teams may anticipate more in held-out evaluation.

JSONL · sample
{"ts":"2026-05-10T14:21:30Z","kind":"deploy",  "service":"payments-svc","version":"v2.14.0","actor":"ci"}
{"ts":"2026-05-10T14:22:01Z","kind":"log",     "service":"checkout-api","level":"error","msg":"timeout calling payments-svc","trace_id":"abc123"}
{"ts":"2026-05-10T14:22:01Z","kind":"metric",  "service":"payments-svc","name":"latency_p99_ms","value":4820}
{"ts":"2026-05-10T14:22:08Z","kind":"trace",   "trace_id":"abc123","spans":[{"svc":"checkout-api","dur_ms":5012},{"svc":"payments-svc","dur_ms":4980}]}
{"ts":"2026-05-10T14:30:00Z","kind":"topology","change":"rename","from":"payments-svc","to":"billing-svc"}
{"ts":"2026-05-10T14:32:11Z","kind":"incident_signal","incident_id":"INC-714","trigger":"alert:checkout-api/error-rate>5%"}
{"ts":"2026-05-10T15:10:00Z","kind":"remediation","incident_id":"INC-714","action":"rollback","target":"billing-svc","version":"v2.13.4","outcome":"resolved"}
Interface · Surface
The engine exposes two operations. The public harness provides a Python adapter base class; teams may implement directly in Python or bridge via subprocess, gRPC, or HTTP to a non-Python engine.

Python
class Engine:
    def ingest(self, events: Iterable[Event]) -> None: ...
    def reconstruct_context(
        self,
        signal: IncidentSignal,
        mode: Literal["fast", "deep"] = "fast",
    ) -> Context: ...
Output · Context Shape
The output of reconstruct_context is a structured object — not free text. Narrative explanations live inside the explain field.

Python
class Context(TypedDict):
    related_events:          list[Event]            # ordered, deduped, with provenance
    causal_chain:            list[CausalEdge]       # (cause_id, effect_id, evidence, confidence)
    similar_past_incidents:  list[IncidentMatch]    # (past_incident_id, similarity, rationale)
    suggested_remediations:  list[Remediation]      # (action, target, historical_outcome, confidence)
    confidence:              float                  # overall, 0..1
    explain:                 str                    # human-readable narrative
Worked Example
Given the JSONL sample above, on receipt of the incident_signal for INC-714, reconstruct_context(signal, mode="fast") must return a Context that:

Related Events
Includes the v2.14.0 deploy, the latency metric, the trace, and the upstream error log — with provenance back to source events.
Causal Chain
Contains an edge deploy → latency spike → upstream error with confidence ≥ 0.5 and evidence pointers.
Similar Past Incidents
The central test. If the engine has previously ingested a payments-svc deploy → rollback pattern, that match must surface despite the payments-svc → billing-svc rename. Topology-independent behavioral matching is non-negotiable.
Suggested Remediation
"Rollback billing-svc to prior version" with confidence reflecting historical success rate.
Latency Budget
Ingest sustained throughput
≥ 1,000 events / sec
Ingest lag (event → queryable)
≤ 5 s
reconstruct_context — fast mode
p95 ≤ 2 s
reconstruct_context — deep mode
p95 ≤ 6 s
Cold-start to first reconstruction
≤ 60 s on the benchmark dataset
Dataset Scale
Two scales. The public bench runs the L2 defaults below — small enough to iterate on a laptop, large enough to surface the rename-robustness test. L3 evaluation runs at higher parameters held by the council.

L2 · Public bench defaults

Time span
7 simulated days
Services
12
Deploys
30
Topology mutations
8 (renames, dependency shifts)
Incidents (train / eval)
24 train · 10 held-out eval
Recurring incident families
5 (each with morphed signatures across renames)
Background events
~200 per service-day (≈17k per default run)
L3 · Adversarial (held out)

Higher service count, denser drift, more incident families per dataset, hand-crafted scenarios (correlated multi-service outages, cascading rename chains). Exact parameters revealed only at final evaluation. You can stress-test your engine at any scale via --n-services and --days.

Concrete Metrics
Each evaluation area maps to an unambiguous number. No marketing scores.

Incident Recall
precision@5 and recall@5 on past-incident matching, computed across the renamed boundary.
Context Quality
F1 of returned related_events vs ground-truth, plus judge-graded 1–5 on a sampled subset.
Pattern Recognition
F1 on incident-family classification after signature morphing.
Temporal Reasoning
% of causal_chain edges with correct ordering and source-precedes-effect under topology drift.
Adaptability
Δ-metric between pre-drift and post-drift evaluation windows. Smaller drop = stronger system.
Latency
Measured p95 vs the budgets above.
Memory Evolution
Improvement in metrics between train-only ingestion and full ingestion.
Explainability
Judge-graded 1–5 on the explain narrative on a sampled subset.
Dependencies
Permitted
Any storage backend (Postgres, DuckDB, Neo4j, Qdrant, custom). Any embedding model. Any LLM provider. All third-party usage cost is borne by the participating team.
Baseline Warning
The SDK ships a reference vector-similarity baseline. Submissions that wrap it without architectural innovation will rank near the bottom by design.
Required
Every dependency disclosed in the README with version pins. Egress to external services declared.
Constraints
Team size
1 – 4
Time
24 hours
Language
Open
Environment
Participant-provided · no compute, cloud, or cost issued by the council
Submission
Repository
Git link with README quickstart.
Runner
bench/run.sh ingests the published sample, runs the canonical scenario, emits a JSON report matching the SDK schema.
Reproducibility
Dockerfile / Nix flake. Held-out eval will run on judges' machines.
Demo
5-minute screen-recorded walkthrough of the worked example.
Writeup
3-page PDF defending: memory representation, relationship-synthesis algorithm, drift-handling strategy, latency engineering, evolution mechanism.
Judging Mechanics
Automated
SDK runs the benchmark against the held-out 20-incident eval set on judges' hardware. Metrics dump to JSON.
Chaos
One hidden scenario, revealed at runtime: a topology shift injected mid-evaluation. System must maintain recall across the shift.
Manual
Explainability and Context Quality graded by panel on sampled outputs.
Live
15-minute Q&A. Expect questions on memory representation, drift handling, and what specifically fails in the baseline.
Bench · Run It Yourself
The full evaluation harness is open and runs on your machine. Pure Python, stdlib only, no network access, no external dependencies. Five steps from clone to score.

Step 1 · Clone
Shell
git clone https://github.com/Sauhard74/Anvil-P-E
cd Anvil-P-E/bench-p02-context
Step 2 · Read the types & interface
Open schema.py for the Event / IncidentSignal / Context TypedDicts, then adapter.py for the three methods every submission must implement: ingest, reconstruct_context, close.
Step 3 · Write your adapter
Create adapters/myteam.py as a thin shim over your engine. For non-Python engines, bridge via subprocess, gRPC, or HTTP.
adapters/myteam.py
from adapter import Adapter
from schema import Event, IncidentSignal, Context

class Engine(Adapter):
    def __init__(self):
        self.store = MyMemorySubstrate()

    def ingest(self, events):
        for e in events:
            self.store.consume(e)

    def reconstruct_context(self, signal: IncidentSignal,
                            mode="fast") -> Context:
        return self.store.reconstruct(signal, mode=mode)

    def close(self):
        self.store.shutdown()
Step 4 · Self-check
Run the condensed multi-seed battery. --quick uses two seeds and a small dataset for fast iteration.
Shell
python self_check.py --adapter adapters.myteam:Engine --quick
You'll see per-metric values (recall@5, precision@5_mean, remediation_acc, latency_p95) and a weighted automated score. Iterate to improve the weak axes.
Step 5 · Full run with arbitrary seeds
When metrics look healthy on --quick, run the full battery and stress-test with any seeds. Each seed runs in a fresh adapter instance — there is no way for caches to leak between seeds.
Shell
python run.py --adapter adapters.myteam:Engine --mode fast \
  --seeds 9999 31415 27182 16180 11235 \
  --n-services 20 --days 14 --out report.json
The bench resists hardcoding by design. Three layers of evaluation:

L1 · Canonical
The worked example shape described above. Passing is necessary but not sufficient.
L2 · Property-based
Pass --seeds any integers. Each seed runs in a freshly-constructed adapter — no cross-seed state leakage, no caching tricks. An engine that handles topology drift passes for any seed; a string-match baseline does not.
L3 · Adversarial
Held-out seeds at higher parameter values, plus hand-crafted scenarios — correlated multi-service outages, cascading rename chains, families morphed across both rename and dependency-graph shifts. Used only at final evaluation. Not distributed.