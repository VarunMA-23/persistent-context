import json
import os
import sys
import time
import random
from dataclasses import dataclass, field
from typing import Any, List, Dict
from datetime import datetime, timedelta

# Setup paths
root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(root, "Anvil-P-E-main", "bench-p02-context"))
sys.path.append(os.path.join(root, "scalartemp-main"))

from adapter import Adapter
from generator import Dataset, GenConfig
from metrics import aggregate, score_match, score_remediation, IncidentScore
from schema import Event, IncidentSignal, Context
from harness import WEIGHTS, LATENCY_BUDGET_MS
from adapters.myteam import Engine as MyEngine

def generate_stress_data(n_noise_lines=100000):
    print(f"Generating stress test data ({n_noise_lines} noise lines + 3 failure families)...")
    
    start_time = datetime(2026, 6, 1, 0, 0, 0)
    
    # 1. Define Incident Families (Patterns)
    families = [
        {
            "name": "DATABASE_FAULT",
            "logs": ["Connection failed to db-01", "Query timeout after 30s", "Database driver error"],
            "trigger": "alert:db_latency_high",
            "remediation": "restart_db_proxy"
        },
        {
            "name": "MEMORY_LEAK",
            "logs": ["JVM Garbage Collection took 5s", "Out of memory error in heap", "Killed by OOMKiller"],
            "trigger": "alert:memory_usage > 95%",
            "remediation": "scale_up_replicas"
        },
        {
            "name": "NETWORK_PARTITION",
            "logs": ["No route to host 10.0.5.2", "Packet loss detected on eth0", "Broken pipe during sync"],
            "trigger": "alert:heartbeat_missing",
            "remediation": "flush_iptables"
        }
    ]

    # 2. Generate Training Data (Reference incidents)
    train_events = []
    for i, fam in enumerate(families):
        ts = start_time + timedelta(hours=i)
        # Signal
        train_events.append({
            "ts": ts.isoformat() + "Z", "kind": "incident_signal", 
            "incident_id": f"TRAIN-FAM-{i}", "service": "main-app", "trigger": fam["trigger"]
        })
        # Logs for fingerprinting
        for log_msg in fam["logs"]:
            train_events.append({
                "ts": (ts - timedelta(minutes=2)).isoformat() + "Z", "kind": "log",
                "service": "main-app", "level": "error", "msg": log_msg
            })
        # Deploy event
        train_events.append({
            "ts": (ts - timedelta(minutes=10)).isoformat() + "Z", "kind": "deploy",
            "service": "main-app", "version": "v1.2.3"
        })
        # Metric spike
        train_events.append({
            "ts": (ts - timedelta(minutes=5)).isoformat() + "Z", "kind": "metric",
            "service": "main-app", "name": "request_latency", "value": 2500.0
        })
        # Remediation
        train_events.append({
            "ts": (ts + timedelta(minutes=5)).isoformat() + "Z", "kind": "remediation",
            "incident_id": f"TRAIN-FAM-{i}", "action": fam["remediation"], "target": "main-app", "outcome": "success"
        })

    # 3. Generate Massive Noise (Evaluation Phase)
    eval_events = []
    eval_signals = []
    ground_truth = []
    
    # Pre-populate with noise
    current_ts = start_time + timedelta(days=1)
    noise_msgs = [
        "Processing request from user-123", "Cache hit for key 'session_abc'", 
        "Heartbeat healthy", "Metrics flushed to collector", "Worker thread-5 idle"
    ]
    
    print("Populating noise...")
    for i in range(n_noise_lines):
        t = current_ts + timedelta(milliseconds=i*50) # Very high frequency
        eval_events.append({
            "ts": t.isoformat() + "Z", "kind": "log",
            "service": "main-app", "level": "info", "msg": random.choice(noise_msgs)
        })

    # 4. Insert Eval Incidents into the Noise
    print("Inserting 9 evaluation incidents and 3 decoys...")
    for j in range(12):
        # Every ~8000 noise lines, insert an incident
        pos = (j + 1) * (n_noise_lines // 13)
        t_inc = current_ts + timedelta(milliseconds=pos*50)
        
        is_decoy = (j >= 9) # Last 3 are decoys
        
        if is_decoy:
            iid = f"DECOY-{j}"
            eval_signals.append({
                "ts": t_inc.isoformat() + "Z", "kind": "incident_signal",
                "incident_id": iid, "service": "main-app", "trigger": "random_spike"
            })
            eval_events.append(eval_signals[-1])
            ground_truth.append({
                "incident_id": iid, "family": None, "expected_remediation": None
            })
        else:
            fam_idx = j % 3
            fam = families[fam_idx]
            iid = f"EVAL-INC-{j}"
            
            # Insert family pattern elements just before signal
            # Deploy
            eval_events.insert(pos - 1, {
                "ts": (t_inc - timedelta(minutes=10)).isoformat() + "Z",
                "kind": "deploy", "service": "main-app", "version": "v1.2.3"
            })
            # Metric
            eval_events.insert(pos - 1, {
                "ts": (t_inc - timedelta(minutes=5)).isoformat() + "Z",
                "kind": "metric", "service": "main-app", "name": "request_latency", "value": 2500.0
            })
            # Logs
            for log_msg in fam["logs"]:
                eval_events.insert(pos - 1, {
                    "ts": (t_inc - timedelta(seconds=10)).isoformat() + "Z",
                    "kind": "log", "service": "main-app", "level": "error", "msg": log_msg
                })
            
            sig = {
                "ts": t_inc.isoformat() + "Z", "kind": "incident_signal",
                "incident_id": iid, "service": "main-app", "trigger": fam["trigger"]
            }
            eval_signals.append(sig)
            eval_events.append(sig)
            ground_truth.append({
                "incident_id": iid, "family": fam_idx, "expected_remediation": fam["remediation"]
            })

    # Sort eval_events by timestamp (important for harness)
    eval_events.sort(key=lambda x: x["ts"])

    return Dataset(train_events, eval_events, eval_signals, ground_truth)

def run_stress_test():
    ds = generate_stress_data(n_noise_lines=100000)
    adapter = MyEngine()
    
    print("\nIngesting training data...")
    adapter.ingest(ds.train_events)
    
    print(f"Running Stress Test Harness on {len(ds.eval_events)} events...")
    
    # Simulating incremental ingestion (Harness style)
    eval_ptr = 0
    scores = []
    
    def ingest_up_to(target_ts):
        nonlocal eval_ptr
        batch = []
        while eval_ptr < len(ds.eval_events) and ds.eval_events[eval_ptr]["ts"] <= target_ts:
            batch.append(ds.eval_events[eval_ptr])
            eval_ptr += 1
        if batch:
            adapter.ingest(batch)

    for sig, gt in zip(ds.eval_signals, ds.ground_truth):
        ingest_up_to(sig["ts"])
        
        t0 = time.monotonic()
        ctx = adapter.reconstruct_context(sig, mode="fast")
        latency_ms = (time.monotonic() - t0) * 1000.0
        
        in_top_k, precision = score_match(ctx, gt, k=5)
        remedy_ok = score_remediation(ctx, gt)
        
        scores.append(IncidentScore(
            incident_id=sig["incident_id"],
            correct_family_in_top_k=in_top_k,
            precision_at_k=precision,
            remediation_matches=remedy_ok,
            latency_ms=latency_ms
        ))

    agg = aggregate(scores)
    
    # Calculate weighted score
    def calc_weighted(res):
        s = 0.0
        s += res["recall@5"] * WEIGHTS["recall@5"]
        s += res["precision@5_mean"] * WEIGHTS["precision@5_mean"]
        s += res["remediation_acc"] * WEIGHTS["remediation_acc"]
        lat_score = 1.0 if res["latency_p95_ms"] <= LATENCY_BUDGET_MS["fast"] else 0.0
        s += lat_score * WEIGHTS["latency_p95_ms"]
        return s

    final_score = calc_weighted(agg)

    print("\n" + "="*60)
    print("S T R E S S   T E S T   R E S U L T S   (100K LOGS)")
    print("="*60)
    for axis, val in agg.items():
        if isinstance(val, (int, float)) and axis != "n":
            print(f"{axis:<20} | {val:.4f}")
    print("-" * 35)
    print(f"{'FINAL ACCURACY':<20} | {final_score:.4f}")
    print("="*60)

if __name__ == "__main__":
    run_stress_test()
