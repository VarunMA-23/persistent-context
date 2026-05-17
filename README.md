# Persistent Context Engine (PCE)
### *Ending the Operational Reasoning Loop for Autonomous SRE*

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Benchmark: Anvil P-02](https://img.shields.io/badge/Benchmark-Anvil--P--02-red.svg)](https://github.com/Sauhard74/Anvil-P-E)

## 🌌 Overview

The **Persistent Context Engine (PCE)** is a high-scale operational memory substrate designed for modern, distributed production environments. Unlike traditional observability tools that store telemetry as isolated, searchable records, PCE continuously synthesizes evolving contextual relationships directly from operational behavior.

It solves the "Systems Problem" of **topology drift** and **operational evolution**: when services are renamed, dependencies shift, and failure signatures morph, PCE preserves the logical reasoning required to resolve incidents without forcing engineers to rebuild causal chains from scratch.

## 🎥 Build Walkthrough & Demo

Explore the complete architecture and watch the Persistent Context Engine build and benchmark in action:

<div align="center">
  <video src="Persistent-Context-Working.mov" width="100%" style="border-radius: 10px; box-shadow: 0 10px 30px rgba(0,0,0,0.3);" controls>
    Your browser does not support the video tag.
  </video>
  
  <p align="center">
    <i>Watch the full 5-minute deep-dive on compilation, causal graph synthesis, and Anvil P-02 evaluations.</i>
  </p>
</div>

---

## 🏗️ Architecture

PCE is built as an append-only, probabilistic memory substrate. It transforms raw telemetry (Logs, Metrics, Traces, Deploys, Topology) into a **Topological-Invariant Directed Acyclic Graph (TI-DAG)**.


```
┌─────────────────────────────────────────────────────────────────────┐
│                         TELEMETRY STREAM                           │
│        Logs • Metrics • Traces • Deploys • Topology Data          │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION PIPELINE                          │
│      Normalization • Parsing • Enrichment • Correlation           │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
╔═════════════════════════════════════════════════════════════════════╗
║                         MEMORY SUBSTRATE                           ║
║         Append-Only Probabilistic Contextual Memory Layer         ║
╠═════════════════════════════════════════════════════════════════════╣
║                                                                     ║
║   ┌─────────────────────┐   ┌─────────────────────┐                ║
║   │ Service Identity    │   │ Baseline Statistics │                ║
║   │ Registry            │   │ & Behavioral Models │                ║
║   └─────────────────────┘   └─────────────────────┘                ║
║                                                                     ║
║                 ┌──────────────────────────┐                        ║
║                 │ Incident Family Registry │                        ║
║                 └──────────────────────────┘                        ║
║                                                                     ║
╚═════════════════════════════════════════════════════════════════════╝
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CAUSAL SYNTHESIS ENGINE                         │
│      Pattern Extraction • Temporal Linking • Dependency Mapping    │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 CAUSAL GRAPH (TI-DAG)                              │
│        Topological-Invariant Directed Acyclic Graph                │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │                               │
                  ▼                               ▼
        ┌───────────────────┐       ┌─────────────────────────┐
        │   Incident Signal │       │    Memory Substrate     │
        └───────────────────┘       └─────────────────────────┘
                  │                               │
                  └───────────────┬───────────────┘
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        CONTEXT COMPILER                            │
│      Multi-Hop Context Retrieval • Semantic Correlation           │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 FAST / DEEP RECONSTRUCTION ENGINE                  │
│        Incident Replay • Root Cause Reconstruction                 │
└─────────────────────────────────────────────────────────────────────┘
                                  │
        ┌─────────────────┬─────────────────┬─────────────────┐
        │                 │                 │                 │
        ▼                 ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌────────────────┐
│ Related Events│ │  Causal Chain │ │Similar Incidents│ │Suggested Fixes│
└───────────────┘ └───────────────┘ └───────────────┘ └────────────────┘
```
---

## 🧠 Core Engineering Principles

### 1. Memory Representation
PCE uses a multi-tiered memory model to balance precision and persistence:
- **Service Identity Registry**: Maps volatile service names to stable **UUID Entity IDs (EIDs)**. It tracks "rename" and "merge" events, ensuring that a causal edge created for `payments-svc` remains valid when it evolves into `billing-svc`.
- **Baseline Statistics**: Maintains rolling metrics (mean, std) for every service-metric pair using a `Welford-like` online algorithm. It computes anomaly z-scores mapped via a sigmoid transform to [0, 1] for unified ranking.
- **Incident Family Registry**: Stores behavioral fingerprints using a three-level hierarchy (**Coarse**, **Shape**, **Exact**). It utilizes a recency-cached **Levenshtein similarity** index for fuzzy behavioral matching.

### 2. Relationship-Synthesis Algorithm
Relationships are synthesized through a **Probabilistic Causal Graph**:
- **Incremental Construction**: Edges are formed at ingest-time based on temporal adjacency, trace spans, and shared context.
- **Bayesian Confidence**: Every edge (Cause → Effect) carries a confidence score that is updated via running averages as new evidence emerges.
- **Entropy-Based Damping**: During retrieval, confidence scores are damped by the logarithmic out-degree of nodes. This prevents "noisy" nodes (e.g., a central database) from dominating the causal narrative, prioritizing specific failure paths over general correlation.

### 3. Drift-Handling Strategy
PCE is engineered to be "Drift-Native":
- **Topology Invariance**: By resolving all telemetry to Entity IDs rather than service names, the system treats infrastructure mutations as metadata updates rather than data breaks.
- **Decoy Detection**: To handle "noisy" environments, the compiler implements a **Temporal Pattern Validator**. It ignores signals that lack a characteristic "Incident Signature" (e.g., Deploy → Metric Degradation → Error Burst), preventing false-positive context reconstruction.
- **Fuzzy Fingerprinting**: Incident signatures are abstracted into topology-independent tokens, allowing the engine to recognize `v2.14 deploy error` as equivalent to a `v1.1 deploy error` from a renamed predecessor.

### 4. Latency Engineering
High-scale operation requires strict latency budgets:
- **Tiered Reconstruction**:
    - **Fast Mode (p95 < 2s)**: Bounded 15-minute lookback with LRU-cached results.
    - **Deep Mode (p95 < 6s)**: Full 30-minute lookback with recursive causal chain expansion and deep past-incident re-ranking.
- **Binary-Search Indexing**: The `EventIndex` provides $O(\log N)$ range queries over the telemetry buffer by indexing service-time pairs, allowing the compiler to gather relevant signals in sub-millisecond time.
- **LRU Context Cache**: Recently reconstructed contexts are cached to handle bursty query patterns often seen during major outages.

### 5. Evolution Mechanism
The substrate "learns" from every interaction:
- **Remediation Feedback**: Successful remediations reinforce the association between an incident fingerprint and a specific action (e.g., `rollback`).
- **Probabilistic Decay**: Unverified causal edges decay over time, while repeated occurrences boost confidence, allowing the graph to prune noise and sharpen its focus on real failure modes.
- **Signature Morphing**: As service behavior evolves across deployments, the `IncidentFamilyRegistry` updates the "Exact" fingerprint while maintaining the "Coarse" family link, preserving historical continuity.

---

## 🛠️ Languages & Technology Stack

- **Core Engine**: Python 3.10+
- **Data Structures**: `collections.deque`, `OrderedDict`, `defaultdict` for high-performance in-memory operations.
- **Algorithms**: 
    - Levenshtein Edit Distance (Fuzzy Search)
    - Bayesian Inference (Confidence Updates)
    - BFS/DFS (Causal Traversal)
    - Welford's Algorithm (Rolling Statistics)

---

## 🚀 Getting Started

### Installation
Run these from a terminal after cloning (same commands on **macOS**, **Linux**, and **Windows** shells such as PowerShell or Git Bash):

```bash
git clone https://github.com/VarunMA-23/persistent-context
cd persistent-context

pip install -r scalartemp-main/requirements.txt
pip install numpy
pip install -e scalartemp-main
```

Use the same `python` / `pip` you will use to run the benchmark (for example **`python3.10`** on macOS/Linux or **`py -3.10`** on Windows).

### Run the Anvil P-02 benchmark (all systems)

**If you want to run from a terminal and get `output.json`:** stay in the **repository root** after `cd`, then use **one** of the command blocks below for your OS. Each run creates **`output.json` here** (same folder as your shell’s current directory) and prints the **same JSON to stdout**; add **`--quiet`** if you do not want the stderr score banners.

Always **`cd`** to the **repository root** first. The harness is `Anvil-P-E-main/bench-p02-context/run.py`. Set **`PYTHONPATH`** to **`.`** so the `scalartemp_main` adapter package (repo root) and the engine resolve correctly.

**Adapter:** `scalartemp_main.adapters.myteam:Engine` (underscore `scalartemp_main` matches the `scalartemp_main/` folder; the engine itself still lives under `scalartemp-main/`.)

**Output:** By default the full JSON report is written to **`output.json`** in the **current working directory** (the folder your terminal is in when you run the command—usually the repo root for the commands above), and the **same JSON is printed to stdout** after the run. Use **`--out path/to/report.json`** for another file path, or **`--out -`** to print JSON to **stdout** only (no file). Use **`--quiet`** to hide stderr banners and progress. Use **`--no-stdout-report`** if you want the report **only** in the file (nothing printed to stdout).

**Score banner (percentage + bars):** Unless you pass **`--quiet`**, the harness prints **Anvil-style banners on stderr**, including a closing line with **weighted score / max** and a **percentage** (the block-style `█` / `★` framing). That is separate from the JSON: the machine-readable report is still **`output.json`** and stdout JSON; the bar display is **stderr only**.

---

**macOS / Linux** (path uses `/`):

```bash
export PYTHONPATH="."
# optional: clear a previous report in this directory
# rm -f output.json
python3.10 Anvil-P-E-main/bench-p02-context/run.py \
  --adapter scalartemp_main.adapters.myteam:Engine \
  --mode fast --seeds 42
```

Same run **without** stderr banners (still writes **`output.json`** and prints JSON to stdout):

```bash
export PYTHONPATH="."
python3.10 Anvil-P-E-main/bench-p02-context/run.py \
  --adapter scalartemp_main.adapters.myteam:Engine \
  --mode fast --seeds 42 --quiet
```

---

**Windows (PowerShell)** (path uses `\`):

```powershell
$env:PYTHONPATH = "."
# optional: Remove-Item -Force output.json
py -3.10 Anvil-P-E-main\bench-p02-context\run.py `
  --adapter scalartemp_main.adapters.myteam:Engine `
  --mode fast --seeds 42
```

With banners suppressed (still writes **`output.json`** in the current directory):

```powershell
$env:PYTHONPATH = "."
py -3.10 Anvil-P-E-main\bench-p02-context\run.py `
  --adapter scalartemp_main.adapters.myteam:Engine `
  --mode fast --seeds 42 --quiet
```

---

**Windows (Command Prompt)**:

```cmd
set PYTHONPATH=.
REM optional: del /f output.json 2>nul
py -3.10 Anvil-P-E-main\bench-p02-context\run.py --adapter scalartemp_main.adapters.myteam:Engine --mode fast --seeds 42
```

Quiet (no stderr banners), same **`output.json`**:

```cmd
set PYTHONPATH=.
py -3.10 Anvil-P-E-main\bench-p02-context\run.py --adapter scalartemp_main.adapters.myteam:Engine --mode fast --seeds 42 --quiet
```

---

**Git Bash on Windows** can use the same **`export PYTHONPATH="."`** and **`python`** / **`py -3.10`** lines as macOS/Linux if your `python` is on `PATH`.

---

### What is in `output.json`?

After a successful run you get **one UTF-8 JSON file** (pretty-printed) with the full benchmark report, including:

| Area | Examples of fields |
|------|----------------------|
| **Run config** | `mode`, `seeds`, `l3_version`, `timestamp` |
| **Results** | `per_seed` (per-seed configs and incident rows), `aggregated` (rolled-up metrics) |
| **Score** | `score.weighted_score`, `score.axes` (e.g. recall@5, latency, remediation) |
| **Provenance** | `adapter`, `adapter_path`, `adapter_sha256` |

By default the **same full JSON** is written to the file **and** printed to **stdout** (identical content). Unless you use **`--quiet`**, you will also see **progress and banners on stderr** before and after the JSON on stdout.

`output.json` is listed in **`.gitignore`** so local runs do not clutter `git status`; rename or copy the file if you need to keep a specific report under version control.

---

**Optional flags**

- **`--out my_report.json`** — write the report to a specific file (default is `output.json`).
- **`--no-stdout-report`** — write only to the file; do not print the JSON to stdout.
- **`--quiet`** — hide stderr banners and progress lines.
- **`--seeds 1 2 3`** — one or more generator seeds (space-separated).
- **`--mode deep`** — deep mode instead of `fast`.

*Replace `42` (or add more seeds) with the values you want to evaluate.*

### Alternate adapter (from inside the bench directory)

If you `cd Anvil-P-E-main/bench-p02-context`, you can use the harness-local adapter (it adds `scalartemp-main/` to `sys.path` automatically). With **`pip install -e scalartemp-main`** already done, run:

```bash
cd Anvil-P-E-main/bench-p02-context
python3.10 run.py --adapter adapters.myteam:Engine --mode fast --seeds 42
```

**Windows (PowerShell):**

```powershell
cd Anvil-P-E-main\bench-p02-context
py -3.10 run.py --adapter adapters.myteam:Engine --mode fast --seeds 42
```

Default **`output.json`** is created in **this** directory (`bench-p02-context/`), not the repo root, because the shell’s current directory changed.

---

## 📊 Performance Targets (Anvil P-02)

| Metric | Target | Current |
| :--- | :--- | :--- |
| **Incident Recall@5** | > 0.90 | 0.92 |
| **Causal Accuracy** | > 0.85 | 0.88 |
| **Fast Mode Latency (p95)** | < 2.0s | 1.4s |
| **Deep Mode Latency (p95)** | < 6.0s | 4.2s |
| **Ingestion Throughput** | > 1k ev/s | 1.8k ev/s |

---

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
