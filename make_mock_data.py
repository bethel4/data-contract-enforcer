import json, uuid, random
from datetime import datetime, timezone, timedelta

def now(): return datetime.now(timezone.utc).isoformat()
def uid(): return str(uuid.uuid4())
def sha(): return ''.join(random.choices('abcdef0123456789', k=64))

# ── WEEK 1: intent_records ──────────────────────────────────────────
w1 = []
for i in range(60):
    w1.append({
        "intent_id": uid(),
        "description": f"Handle user authentication step {i}",
        "code_refs": [{"file": f"src/auth/handler_{i}.py", "line_start": 10, "line_end": 40, "symbol": f"handle_auth_{i}", "confidence": round(random.uniform(0.6, 0.99), 2)}],
        "governance_tags": random.sample(["auth", "pii", "billing", "logging"], 2),
        "created_at": now()
    })
with open("outputs/week1/intent_records.jsonl", "w") as f:
    [f.write(json.dumps(r)+"\n") for r in w1]
print("Week 1: OK")

# ── WEEK 2: verdicts ────────────────────────────────────────────────
w2 = []
for i in range(60):
    score = random.randint(2, 5)
    w2.append({
        "verdict_id": uid(),
        "target_ref": f"src/auth/handler_{i}.py",
        "rubric_id": sha(),
        "rubric_version": "1.2.0",
        "scores": {"code_quality": {"score": score, "evidence": ["code looks good"], "notes": "auto-generated"}},
        "overall_verdict": random.choice(["PASS", "FAIL", "WARN"]),
        "overall_score": round(score * 0.8 + random.uniform(0, 0.5), 2),
        "confidence": round(random.uniform(0.7, 0.99), 2),
        "evaluated_at": now()
    })
with open("outputs/week2/verdicts.jsonl", "w") as f:
    [f.write(json.dumps(r)+"\n") for r in w2]
print("Week 2: OK")

# ── WEEK 3: extractions ─────────────────────────────────────────────
w3 = []
for i in range(60):
    entity_id = uid()
    w3.append({
        "doc_id": uid(),
        "source_path": f"/docs/document_{i}.pdf",
        "source_hash": sha(),
        "extracted_facts": [
            {"fact_id": uid(), "text": f"Important fact number {j} from document {i}", "entity_refs": [entity_id], "confidence": round(random.uniform(0.6, 0.99), 2), "page_ref": random.randint(1, 20), "source_excerpt": f"verbatim excerpt {j}"}
            for j in range(3)
        ],
        "entities": [{"entity_id": entity_id, "name": f"Entity {i}", "type": random.choice(["PERSON","ORG","LOCATION","DATE","AMOUNT","OTHER"]), "canonical_value": f"canonical_{i}"}],
        "extraction_model": "claude-3-5-sonnet-20241022",
        "processing_time_ms": random.randint(500, 3000),
        "token_count": {"input": random.randint(3000, 5000), "output": random.randint(500, 1200)},
        "extracted_at": now()
    })
with open("outputs/week3/extractions.jsonl", "w") as f:
    [f.write(json.dumps(r)+"\n") for r in w3]
print("Week 3: OK")

# ── WEEK 4: lineage_snapshots ───────────────────────────────────────
nodes = [
    {"node_id": "file::src/week3/extractor.py", "type": "FILE", "label": "extractor.py", "metadata": {"path": "src/week3/extractor.py", "language": "python", "purpose": "Extracts facts from documents", "last_modified": now()}},
    {"node_id": "file::src/week4/cartographer.py", "type": "FILE", "label": "cartographer.py", "metadata": {"path": "src/week4/cartographer.py", "language": "python", "purpose": "Maps codebase lineage", "last_modified": now()}},
    {"node_id": "file::outputs/week3/extractions.jsonl", "type": "TABLE", "label": "extractions.jsonl", "metadata": {"path": "outputs/week3/extractions.jsonl", "language": "jsonl", "purpose": "Extracted facts output", "last_modified": now()}},
    {"node_id": "file::outputs/week4/lineage_snapshots.jsonl", "type": "TABLE", "label": "lineage_snapshots.jsonl", "metadata": {"path": "outputs/week4/lineage_snapshots.jsonl", "language": "jsonl", "purpose": "Lineage graph output", "last_modified": now()}}
]
edges = [
    {"source": "file::src/week3/extractor.py", "target": "file::outputs/week3/extractions.jsonl", "relationship": "PRODUCES", "confidence": 0.95},
    {"source": "file::src/week4/cartographer.py", "target": "file::outputs/week4/lineage_snapshots.jsonl", "relationship": "PRODUCES", "confidence": 0.95},
    {"source": "file::outputs/week3/extractions.jsonl", "target": "file::src/week4/cartographer.py", "relationship": "READS", "confidence": 0.88}
]
w4 = [{"snapshot_id": uid(), "codebase_root": "/workspace", "git_commit": "a"*40, "nodes": nodes, "edges": edges, "captured_at": now()}]
with open("outputs/week4/lineage_snapshots.jsonl", "w") as f:
    [f.write(json.dumps(r)+"\n") for r in w4]
print("Week 4: OK")

# ── WEEK 5: events ──────────────────────────────────────────────────
agg_id = uid()
w5 = []
for i in range(60):
    ts_occ = datetime.now(timezone.utc) - timedelta(seconds=60-i)
    ts_rec = ts_occ + timedelta(seconds=1)
    w5.append({
        "event_id": uid(), "event_type": "DocumentProcessed", "aggregate_id": agg_id,
        "aggregate_type": "Document", "sequence_number": i+1,
        "payload": {"status": "complete", "doc_id": uid()},
        "metadata": {"causation_id": None, "correlation_id": uid(), "user_id": "user_001", "source_service": "week3-document-refinery"},
        "schema_version": "1.0",
        "occurred_at": ts_occ.isoformat(),
        "recorded_at": ts_rec.isoformat()
    })
with open("outputs/week5/events.jsonl", "w") as f:
    [f.write(json.dumps(r)+"\n") for r in w5]
print("Week 5: OK")

# ── LANGSMITH traces ─────────────────────────────────────────────────
traces = []
for i in range(60):
    pt = random.randint(2000, 5000)
    ct = random.randint(200, 1000)
    t_start = datetime.now(timezone.utc) - timedelta(seconds=60-i)
    t_end = t_start + timedelta(seconds=random.randint(1, 4))
    traces.append({
        "id": uid(), "name": f"extraction_chain_{i}", "run_type": random.choice(["llm","chain","tool"]),
        "inputs": {"doc": f"doc_{i}"}, "outputs": {"facts": 3}, "error": None,
        "start_time": t_start.isoformat(), "end_time": t_end.isoformat(),
        "total_tokens": pt+ct, "prompt_tokens": pt, "completion_tokens": ct,
        "total_cost": round((pt+ct) * 0.000003, 4),
        "tags": ["week3", "extraction"], "parent_run_id": None, "session_id": uid()
    })
with open("outputs/traces/runs.jsonl", "w") as f:
    [f.write(json.dumps(r)+"\n") for r in traces]
print("Traces: OK")
print("\nAll mock data created! Run: ls outputs/week*/  to verify")