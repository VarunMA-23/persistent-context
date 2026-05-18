# Persistent Context Engine (PCE): Architecture & Deep Dive

The **Persistent Context Engine (PCE)** is a high-scale operational memory substrate designed for autonomous Site Reliability Engineering (SRE). Unlike traditional observability databases that treat telemetry (logs, metrics, traces, deployments, and topology events) as fragmented, short-horizon searchable records, PCE continuously synthesizes evolving contextual relationships directly from operational behavior. 

This document provides a comprehensive technical exploration of the engine's core components, design philosophies, relationship synthesis, and query-time reconstruction algorithms.

---

## 🌌 The Systems Problem & Design Philosophy

Modern distributed production environments suffer from three main vectors of disruption that degrade standard machine-learning and observability tools:

1. **Topology Drift**: Microservices are constantly renamed, split, or merged. Static dependency graphs break instantly under rename events, making historical incidents and causal paths appear unrelated.
2. **Operational Evolution**: Failure signatures morph. An error in a service version `v1` may reappear in version `v2` under a slightly different telemetry pattern, a different metric range, or on a renamed service.
3. **Telemetry Noise and False Alerts (Decoys)**: Real incidents are accompanied by secondary alerts, while benign anomalies ("decoys") can trigger false-positive root-cause investigations.

### The PCE Paradigm: Memory Substrate vs. Observability Store
PCE addresses these issues by modeling operational context as an append-only **Topological-Invariant Directed Acyclic Graph (TI-DAG)** combined with a **probabilistic memory substrate**. 

By decoupling physical identifiers (e.g., service names) from logical entities (e.g., stable UUIDs) and evaluating behavioral sequences rather than exact string/metric matches, the engine achieves **drift-invariance** and **continuous behavioral reinforcement**.

---

## 🏗️ End-to-End System Architecture

The engine is structured as a pipeline with four primary layers: **Ingestion**, **Probabilistic Memory Substrate**, **Causal Relationship Synthesis**, and **Adaptive Context Compilation**.

```
                           [ TELEMETRY STREAM ]
      Logs • Metrics • Traces • Deployments • Topology Mutations
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │     TelemetryBuffer      │ (Append-Only Circular Queue,
                     │  & Inverse Document Freq │  O(log N) Indexed Queries)
                     └────────────┬─────────────┘
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │       RulesEngine        │ (Ingest-time causal rules
                     │  (Stateful & Parallel)   │  & baseline updates)
                     └────────────┬─────────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         ▼                        ▼                        ▼
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ ServiceIdentity  │    │  BaselineStats   │    │  CausalGraph     │
│ Stable UUID EIDs │    │ Welford Rolling  │    │  Probabilistic   │
│ & Aliases        │    │ metric z-scores  │    │  Temporal DAG    │
└──────────────────┘    └──────────────────┘    └──────────────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │     ContextCompiler      │ (Incident-time context
                     │  (Decoy-Aware Traversal) │  reconstruction)
                     └────────────┬─────────────┘
                                  │
                                  ▼
                         [ Context Object ]
       Related Events • Causal Chain • Past Matches • Suggestions
```

---

## 🧠 Deep Dive: The Probabilistic Memory Substrate

The memory substrate is stateful, maintaining the persistent representations that allow the system to recognize recurring patterns despite infrastructure changes.

### 1. Service Identity Registry (`ServiceIdentity`)
To isolate the engine from service renames and splits, the identity registry maps ephemeral service name strings to stable **Entity IDs (EIDs)** represented as UUIDs.
* **Rename Handling**: When a topology event of type `rename` is ingested (e.g., mapping `payments-svc` to `billing-svc`), the registry updates its internal mappings so that the new name points to the same underlying UUID EID.
* **Historical Aliasing**: A single logical entity keeps track of every name it has ever used, enabling the engine to trace historical metrics and trace paths through rename boundaries.
* **Equivalence Validation**: Two names are verified as equivalent if they map to the same EID, ensuring that any past causal edge remains valid for the renamed service.

### 2. Baseline Statistics (`BaselineStats`)
Metrics vary widely across environments and services. Rather than relying on hardcoded thresholds, PCE measures metric anomalousness dynamically.
* **Welford's Online Algorithm**: For every metric of every service (tracked by EID), the engine maintains a rolling baseline using a Welford-like algorithm. It updates the sum and sum of squares over a rolling window (default $1,000$ points) to compute:
  $$\mu = \frac{1}{N}\sum x_i, \quad \sigma = \sqrt{\frac{1}{N}\sum (x_i - \mu)^2}$$
* **Minimum Variance Saturation**: To prevent mathematical division-by-zero or saturation in extremely stable environments where standard deviation approaches zero, a floor is set:
  $$\sigma_{\text{min}} = \max(10^{-4}, (0.05 \times |\mu|)^2)$$
* **Sigmoid Anomaly Mapping**: Raw z-scores are translated into a normalized $[0, 1]$ anomaly score using a sigmoid function:
  $$S(z) = \frac{1}{1 + e^{-0.8 \times (|z| - 1.5)}}$$
  This maps $z=0 \rightarrow 0.27$ (neutral), $z=2 \rightarrow 0.73$ (elevated), and $z=3 \rightarrow 0.95$ (extreme anomaly), creating a standard scale for ranking.

### 3. Incident Family Registry & Fingerprinter (`IncidentFamilyRegistry`)
When an incident is reported, PCE abstracts the preceding event sequence into a topology-invariant **behavioral fingerprint**.
* **Behavioral Event Abstraction**: Events in the lookback window are mapped to single-character abstract codes:
  * `D`: Deployment
  * `A`: Metric Latency Spike (latency/duration $> 500\text{ms}$)
  * `X`: Metric Error Rate Spike (failure rate $> 0$)
  * `E`: Error Log
  * `R`: Remediation Resolved
* **Deduplication**: Sequences are compressed by limiting duplicate consecutive codes to prevent index inflation.
* **Three-Level Signature Hierarchy**:
  * **Coarse Archetype**: High-level classification (e.g., `deploy_failure` if deployment and error logs exist, `upstream_cascade` if metric latency and upstream errors occur).
  * **Behavioral Shape**: Chronological code string stripping entity details (e.g., `DAE` representing Deploy $\rightarrow$ Latency Spike $\rightarrow$ Error Log).
  * **Exact Sequence**: Fully resolved sequence including stable EID markers.
* **Fuzzy Levenshtein Indexing**: Stored fingerprints are queried using normalized Levenshtein similarity:
  $$\text{Sim}(s_1, s_2) = 1.0 - \frac{\text{LevenshteinDistance}(s_1, s_2)}{\max(|s_1|, |s_2|)}$$
  During retrieval, the similarity is boosted by $+20\%$ if the primary EIDs match, encouraging entity-specific matches while allowing cross-service behavioral fallback.

### 4. Bayesian Remediation Store (`RemediationStore`)
To suggest action pathways, the engine tracks the results of all applied remediation events.
* **Confidence Tuning**: If a remediation `action` on a `target` service resolves an incident, the confidence score for that pair is reinforced. If it fails, the confidence is penalized.
* **Prioritization**: When suggesting remediations, PCE looks up the best-matching historical incidents in the registry and extracts the successfully applied actions. As a fallback, it suggests remediations based on the global Bayesian confidence scores for that service.

---

## ⚡ Ingest-Time Relationship Synthesis & Rules Engine

Ingested events are appended to a circular circular buffer, `TelemetryBuffer` (which tracks up to $500,000$ events). An in-memory inverse document frequency index (`EventIndex`) measures the rarity of log messages and metric names to weigh their impact. 

As events stream in, the `RulesEngine` processes them against four stateful rules to build the `CausalGraph` incrementally.

```
       [ Ingested Event ]
               │
      ┌────────┴────────┬─────────────────────────┬────────────────────────┐
      ▼                 ▼                         ▼                        ▼
 [ Rule 1: Temporal ] [ Rule 2: Correlation ] [ Rule 3: Deployment ] [ Rule 4: Family ]
  - Checks past 60s    - Pearson ρ on 10s      - Scans forward 15m   - Extracts preceding
    lookback range      binned metrics over     for error logs /       event sequence
  - Decays confidence   5-min window            incidents            - Registers exact
    with time gap      - ρ > 0.70              - Link deploy to       behavioral shape
  - Links metric/log   - Link earliest events   failure events         in registry
```

### Rule 1: Temporal Proximity
Links metrics, logs, and traces to preceding deployments, metrics, or logs within strict, decaying time windows.
* **Windows & Base Confidences**:
  * `deploy` $\rightarrow$ `metric`: 5.0 seconds window (Base: 0.90)
  * `deploy` $\rightarrow$ `log`: 30.0 seconds window (Base: 0.75)
  * `metric` $\rightarrow$ `log`: 90.0 seconds window (Base: 0.70)
* **Confidence Decay**: Confidence decays linearly with the time gap:
  $$C = C_{\text{base}} \times \left(1.0 - \frac{\Delta t}{W_{\text{sec}}}\right)$$

### Rule 2: Correlated Degradation
Computes physical correlation between distinct services.
* **Pearson Correlation ($\rho$)**: Upon ingesting a metric event, the rule bins the metric's values and those of other active services into 10-second intervals over a 5-minute rolling window.
* **Threshold**: If $\rho \ge 0.70$ over a minimum of 5 data points, a `correlation` edge is created pointing from the earliest correlated service event to the triggered metric, indicating shared degradation.

### Rule 3: Deployment Adjacency
Captures failures caused by new code.
* **Forward Scanning**: When a `deploy` event is ingested, the engine registers a forward scan of up to 15 minutes.
* **Link Formation**: If a subsequent error log or incident signal occurs on that service (or any of its historical aliases), a `deployment` edge is recorded.

### Rule 4: Incident Family Extraction
When an `incident_signal` arrives, the rules engine extracts a 35-minute preceding sequence of events matching the trigger service, converts it into coarse, shape, and exact fingerprints, and registers it in the `IncidentFamilyRegistry`.

---

## 🔍 The Append-Only Probabilistic Causal Graph

The `CausalGraph` stores all synthesized edges (`CausalEdge`). Edges are append-only and never rebuilt. If an existing (cause, effect, type) edge is registered again, its confidence is updated via a Bayesian-style running average.

### Entropy-Based Graph Damping
A major failure in large systems is "hub domination"—central nodes like databases or shared queues generate thousands of correlated alerts, drowning out the actual root cause. To counter this, PCE implements **Entropy-Based Graph Damping** at retrieval time.
* **Degree Penalty**: The out-degree $k$ of a node (the number of edges connected to it) is used to damp edge confidence scores during BFS/DFS traversal:
  $$C_{\text{damp}} = \min\left(1.0, \frac{C_{\text{raw}}}{\max(1.0, \log_{10}(k + 1))}\right)$$
* **Impact**: Highly connected, noisy nodes have their edge confidence heavily penalized, allowing specific, low-entropy causal paths to stand out during investigation.

---

## 🚀 Incident-Time Adaptive Context Compilation

During an incident investigation, the `ContextCompiler` reconstructs the context dynamically using a tiered latency budget: **Fast Mode** (bounds lookback to 15 minutes, using LRU caches for sub-2 second latency) and **Deep Mode** (looks back 30 minutes, recursively expanding causal chains for sub-6 second latency).

### 1. Decoy Signal Detection
To prevent false positives from transient anomalies or bad alerts, the compiler validates whether the incoming signal has a real incident signature.
* **Lookback Verification**: It scans a 35-minute lookback window for:
  1. A deployment on the trigger service (`has_deploy`)
  2. A metric latency spike ($>500\text{ms}$) on the service (`has_metric_spike`)
  3. An error log from or mentioning the service (`has_error_log`)
* **Minimal Context Return**: If less than two of these three conditions are met, the engine marks the signal as a **decoy** and returns a minimal context object with low confidence (0.1) and a note, avoiding costly graph search and false-positive past matches.

### 2. Multi-Hop Context Gathering
To compile a complete picture of the event, the compiler gathers signals across multiple vectors:
1. **Trigger Service Events**: Collects all logs, metrics, and traces for the service and its aliases.
2. **Topology Events**: Includes renames or dependency shifts in the window.
3. **Log & Trace Cross-References**: Scans trace spans to discover downstream service IDs, and parses error log text to find references to other services.
4. **Causal Graph Expansion**: Traverses the `CausalGraph` forward and backward from all gathered event indices to find connected signals.

### 3. Unified Anomaly Ranking (`AnomalyScorer`)
Gathered events are scored and ranked. The top 50 are selected as the final `related_events`.
* **Log Scoring**: Scaled by log level (`error` = 0.95, `warn` = 0.65) and multiplied by its IDF weight (less common logs score higher).
* **Metric Scoring**: Relies on the sigmoid-mapped anomaly score from the baseline stats, boosted by the metric's IDF weight.

### 4. Bayesian Remediation & Past Incident Selection
The compiler extracts the exact fingerprint of the current incident and queries the family registry. 
* **Candidate Retrieval**: Fetches similar past incidents matching the fingerprint sequence.
* **Reranking**: Priority is sorted by:
  1. Root cause entity match ($+20\%$ boost)
  2. Any entity match (surviving renames)
  3. Upstream dependency match
  4. Availability of historically validated remediation actions
* **Suggested Remediations**: The system fetches successful remediation actions from these matched incidents. It ranks them and provides a confidence score based on the historical success rates in the store.

### 5. Concept Focus: Incident Neighbors (Behavioral vs. Topological Adjacency)
A core innovation of the PCE is its dual-nature mapping of **Incident Neighbors** to isolate issues and retrieve precise history:
* **Behavioral Neighbors (Fuzzy Fingerprint Proximity)**: Rather than performing database search by exact string/tag queries, the engine performs a **nearest-neighbor search in behavioral space**. By computing the Levenshtein distance over event archetypes (e.g., `DAE` sequences), it retrieves the closest "behavioral neighbors" (similar past incidents). This provides resilient past-remediation recommendations across topology renaming events.
* **Topological Neighbors (Dependency Causal Adjacency)**: When an incident is analyzed, the compiler expands the context through the causal graph to include the **topological neighbors** (upstream callers and downstream dependencies).
* **Noisy Neighbor Mitigation**: To prevent shared infrastructure hubs (databases, message queues) from generating a flood of correlated alerts that drown out other topological neighbors, the traversal applies **Entropy-Based Graph Damping**. High out-degree hubs are penalized, preserving the context of specific local neighbors.

---

## 📊 Performance and Complexity Bounds

| Component / Operation | Time Complexity | Space Complexity | Latency Budget |
| :--- | :--- | :--- | :--- |
| **Ingestion (`TelemetryBuffer.append`)** | $\mathcal{O}(1)$ (circular buffer write) | $\mathcal{O}(M)$ (capped at $500\text{k}$ events) | $< 1\text{ms}$ |
| **Rules Processing (`RulesEngine.process`)** | $\mathcal{O}(L)$ (limited backward scanning range) | $\mathcal{O}(E)$ (proportional to edge count) | $< 3\text{ms}$ per event |
| **Fuzzy Matching (`Levenshtein`)** | $\mathcal{O}(|s_1| \cdot |s_2|)$ | $\mathcal{O}(\min(|s_1|, |s_2|))$ DP table | $< 15\text{ms}$ |
| **Fast Reconstruction Mode** | $\mathcal{O}(E \log V)$ (bounded BFS + index lookup) | $\mathcal{O}(V + E)$ (sub-graph extraction) | $\text{p95} \le 1.4\text{s}$ |
| **Deep Reconstruction Mode** | $\mathcal{O}(E \log V + R)$ (extended depth BFS + reranking) | $\mathcal{O}(V + E)$ (expanded sub-graph) | $\text{p95} \le 4.2\text{s}$ |

---
