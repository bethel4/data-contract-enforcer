"""
AI Contract Extensions — Week 7 Data Contract Enforcer

Extension 1: Embedding Drift Detection (cosine distance on text centroid)
Extension 2: Prompt Input Schema Validation (JSON Schema, quarantine on failure)
Extension 3: LLM Output Schema Violation Rate (trend tracking for Week 2 verdicts)
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from openrouter_client import call_openrouter, load_env
    load_env()
    OPENROUTER_AVAILABLE = True
except Exception:
    OPENROUTER_AVAILABLE = False



def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_jsonl(path: str) -> list:
    rows = []
    p = Path(path)
    if not p.exists():
        return rows
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


# ── Extension 1: Embedding Drift ─────────────────────────────────────────────

def simple_embed(text: str, dim: int = 128) -> np.ndarray:
    """
    Char-frequency embedding fallback (no API key needed).
    When OPENROUTER_API_KEY is set, real_embed() is used instead.
    """
    vec = np.zeros(dim, dtype=np.float32)
    text = text.lower()[:500]
    for i, ch in enumerate(text):
        vec[ord(ch) % dim] += 1.0
    for i in range(len(text) - 1):
        vec[(ord(text[i]) * 31 + ord(text[i+1])) % dim] += 0.5
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def real_embed(text: str) -> np.ndarray:
    """
    Real embedding via OpenRouter (uses openai/text-embedding-ada-002 through OR).
    Falls back to simple_embed if API key missing or call fails.
    """
    import os, json
    if not OPENROUTER_AVAILABLE or not os.environ.get("OPENROUTER_API_KEY",""):
        return simple_embed(text)
    try:
        import urllib.request
        payload = json.dumps({
            "model": "openai/text-embedding-ada-002",
            "input": text[:2000]
        }).encode()
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/embeddings",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
                "HTTP-Referer": "https://github.com/week7-enforcer",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            vec = np.array(data["data"][0]["embedding"], dtype=np.float32)
            norm = np.linalg.norm(vec)
            return vec / norm if norm > 0 else vec
    except Exception as e:
        return simple_embed(text)


def get_embed(text: str) -> np.ndarray:
    """Auto-select: real embedding if API key present, else char-frequency."""
    import os
    if OPENROUTER_AVAILABLE and os.environ.get("OPENROUTER_API_KEY",""):
        return real_embed(text)
    return simple_embed(text)


def cosine_distance(a: np.ndarray, b: np.ndarray) -> float:
    sim = float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-9))
    return round(1.0 - sim, 6)


def check_embedding_drift(
    extraction_rows: list,
    baseline_path:   str = "schema_snapshots/embedding_baselines.npz",
    threshold:       float = 0.15,
) -> dict:
    texts = []
    for r in extraction_rows:
        for fact in r.get("extracted_facts", []):
            t = fact.get("text", "")
            if t:
                texts.append(t)

    sample = texts[:200] if len(texts) >= 200 else texts
    if not sample:
        return {"status": "ERROR", "message": "No text values found", "drift_score": None}

    embed_fn = get_embed if (OPENROUTER_AVAILABLE and __import__('os').environ.get('OPENROUTER_API_KEY','')) else simple_embed
    embeddings       = np.array([embed_fn(t) for t in sample])
    current_centroid = embeddings.mean(axis=0)

    bp = Path(baseline_path)
    bp.parent.mkdir(parents=True, exist_ok=True)

    if not bp.exists():
        np.savez(str(bp), centroid=current_centroid)
        print(f"  Embedding baseline created: {len(sample)} texts sampled → {baseline_path}")
        return {
            "status":      "BASELINE_SET",
            "drift_score": 0.0,
            "threshold":   threshold,
            "sample_size": len(sample),
            "note":        "Baseline set. Run again to detect drift.",
        }

    baseline_centroid = np.load(str(bp))["centroid"]
    drift = cosine_distance(current_centroid, baseline_centroid)
    status = "FAIL" if drift > threshold else ("WARN" if drift > threshold * 0.6 else "PASS")
    print(f"  Embedding drift: {drift:.4f} (threshold={threshold}) → {status}")
    return {
        "status":      status,
        "drift_score": drift,
        "threshold":   threshold,
        "sample_size": len(sample),
    }


# ── Extension 2: Prompt Input Schema Validation ───────────────────────────────

PROMPT_INPUT_SCHEMA = {
    "required": ["doc_id", "source_path"],
    "properties": {
        "doc_id":          {"type": str, "min_len": 36, "max_len": 36},
        "source_path":     {"type": str, "min_len": 1},
        "extraction_model":{"type": str},
        "processing_time_ms": {"type": int, "min_val": 0},
    },
}


def validate_prompt_input(record: dict) -> list:
    errors = []
    for field in PROMPT_INPUT_SCHEMA["required"]:
        if field not in record or record[field] is None:
            errors.append(f"Missing required field: {field}")
    for field, rules in PROMPT_INPUT_SCHEMA["properties"].items():
        val = record.get(field)
        if val is None:
            continue
        if not isinstance(val, rules["type"]):
            errors.append(f"Field '{field}' has wrong type: expected {rules['type'].__name__}, got {type(val).__name__}")
        if rules.get("min_len") and isinstance(val, str) and len(val) < rules["min_len"]:
            errors.append(f"Field '{field}' too short: {len(val)} < {rules['min_len']}")
        if rules.get("max_len") and isinstance(val, str) and len(val) > rules["max_len"]:
            errors.append(f"Field '{field}' too long: {len(val)} > {rules['max_len']}")
        if rules.get("min_val") is not None and isinstance(val, (int, float)) and val < rules["min_val"]:
            errors.append(f"Field '{field}' below minimum: {val} < {rules['min_val']}")
    return errors


def check_prompt_input_schema(
    extraction_rows: list,
    quarantine_dir:  str = "outputs/quarantine",
) -> dict:
    valid, invalid = 0, 0
    quarantine_records = []

    for r in extraction_rows:
        errors = validate_prompt_input(r)
        if errors:
            invalid += 1
            r["_validation_errors"] = errors
            quarantine_records.append(r)
        else:
            valid += 1

    if quarantine_records:
        Path(quarantine_dir).mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        q_path = Path(quarantine_dir) / f"{ts}.jsonl"
        with open(q_path, "w") as f:
            for r in quarantine_records:
                f.write(json.dumps(r) + "\n")
        print(f"  Quarantined {invalid} records → {q_path}")
    else:
        print(f"  All {valid} prompt inputs valid")

    return {
        "valid":   valid,
        "invalid": invalid,
        "quarantine_path": str(Path(quarantine_dir) / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.jsonl") if quarantine_records else None,
        "status":  "FAIL" if invalid > 0 else "PASS",
    }


# ── Extension 3: LLM Output Schema Violation Rate ────────────────────────────

VERDICT_SCHEMA = {
    "required": ["verdict_id", "overall_verdict", "overall_score", "confidence"],
    "verdict_enum": {"PASS", "FAIL", "WARN"},
    "score_range": (1, 5),
    "confidence_range": (0.0, 1.0),
}


def check_llm_output_schema(
    verdict_rows:   list,
    baseline_rate:  float | None = None,
    warn_threshold: float = 0.02,
) -> dict:
    total      = len(verdict_rows)
    violations = 0

    for v in verdict_rows:
        bad = False
        for field in VERDICT_SCHEMA["required"]:
            if field not in v or v[field] is None:
                bad = True
                break
        if not bad and v.get("overall_verdict") not in VERDICT_SCHEMA["verdict_enum"]:
            bad = True
        if not bad:
            score = v.get("overall_score", 0)
            if not (isinstance(score, (int, float)) and 0 <= score <= 5):
                bad = True
        if not bad:
            conf = v.get("confidence", 0)
            if not (isinstance(conf, (int, float)) and 0.0 <= conf <= 1.0):
                bad = True
        for crit_val in v.get("scores", {}).values():
            s = crit_val.get("score", 0) if isinstance(crit_val, dict) else 0
            if not (isinstance(s, int) and 1 <= s <= 5):
                bad = True
        if bad:
            violations += 1

    rate  = round(violations / max(total, 1), 6)
    trend = "unknown"
    if baseline_rate is not None:
        if rate > baseline_rate * 1.5:
            trend = "rising"
        elif rate < baseline_rate * 0.8:
            trend = "falling"
        else:
            trend = "stable"

    status = "WARN" if (rate > warn_threshold or trend == "rising") else "PASS"
    print(f"  LLM output violations: {violations}/{total} rate={rate:.4f} trend={trend} → {status}")
    return {
        "total_outputs":     total,
        "schema_violations": violations,
        "violation_rate":    rate,
        "trend":             trend,
        "baseline_rate":     baseline_rate,
        "status":            status,
    }


# ── Main runner ───────────────────────────────────────────────────────────────

def run_ai_extensions(
    extractions_path: str,
    verdicts_path:    str,
    output_path:      str = "ai_metrics.json",
    mode:             str = "AUDIT",
) -> dict:
    print(f"\nRunning AI Contract Extensions (mode={mode})...")
    rows     = load_jsonl(extractions_path)
    verdicts = load_jsonl(verdicts_path)

    print(f"\n[Extension 1] Embedding drift ({len(rows)} extraction docs)")
    drift_result = check_embedding_drift(rows)

    print(f"\n[Extension 2] Prompt input schema ({len(rows)} records)")
    prompt_result = check_prompt_input_schema(rows)

    print(f"\n[Extension 3] LLM output schema ({len(verdicts)} verdicts)")
    llm_result = check_llm_output_schema(verdicts)

    overall = "FAIL" if any(
        r.get("status") == "FAIL" for r in [drift_result, prompt_result, llm_result]
    ) else ("WARN" if any(
        r.get("status") == "WARN" for r in [drift_result, prompt_result, llm_result]
    ) else "PASS")

    metrics = {
        "run_date":               now(),
        "mode":                   mode,
        "embedding_drift":        drift_result,
        "prompt_input_validation":prompt_result,
        "llm_output_schema":      llm_result,
        "overall_ai_status":      overall,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)

    # Write violation to log if WARN/FAIL and mode is not AUDIT
    if overall in ("WARN","FAIL") and mode != "AUDIT":
        Path("violation_log").mkdir(exist_ok=True)
        with open("violation_log/violations.jsonl", "a") as f:
            f.write(json.dumps({
                "violation_id":  str(uuid.uuid4()),
                "check_id":      "ai_extensions.overall",
                "detected_at":   now(),
                "severity":      "HIGH" if overall == "FAIL" else "MEDIUM",
                "message":       f"AI extensions status: {overall}",
                "type":          "ai_contract",
                "blame_chain":   [],
                "blast_radius":  {"affected_nodes":[], "estimated_records": 0},
            }) + "\n")

    print(f"\nAI metrics → {output_path}  (overall: {overall})")
    return metrics


def main():
    ap = argparse.ArgumentParser(
        description="AI Contract Extensions — embedding drift, prompt validation, LLM output schema"
    )
    ap.add_argument("--extractions", default="outputs/week3/extractions.jsonl")
    ap.add_argument("--verdicts",    default="outputs/week2/verdicts.jsonl")
    ap.add_argument("--output",      default="ai_metrics.json")
    ap.add_argument("--mode",        default="AUDIT", choices=["AUDIT","WARN","ENFORCE"])
    args = ap.parse_args()
    run_ai_extensions(args.extractions, args.verdicts, args.output, args.mode)


if __name__ == "__main__":
    main()