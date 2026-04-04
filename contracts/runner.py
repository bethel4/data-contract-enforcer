"""
ValidationRunner — Week 7 Data Contract Enforcer
Executes every clause in a contract YAML against a data snapshot.
Produces a structured JSON report with PASS/FAIL/WARN/ERROR per check.
Detects statistical drift against stored baselines.
Never crashes — missing columns return ERROR status and execution continues.
"""

import argparse
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


# ── helpers ───────────────────────────────────────────────────────────────────

def load_jsonl(path: str) -> list:
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def flatten(rows: list) -> pd.DataFrame:
    flat = []
    for r in rows:
        row = {}
        for k, v in r.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                row[k] = v
            elif isinstance(v, list):
                row[f"{k}__count"] = len(v)
                if v and isinstance(v[0], dict):
                    for sk, sv in v[0].items():
                        if isinstance(sv, (str, int, float, bool)) or sv is None:
                            row[f"{k}_0_{sk}"] = sv
            elif isinstance(v, dict):
                for sk, sv in v.items():
                    if isinstance(sv, (str, int, float, bool)) or sv is None:
                        row[f"{k}_{sk}"] = sv
        flat.append(row)
    return pd.DataFrame(flat)


def find_col(col: str, df: pd.DataFrame) -> str | None:
    """Find the actual column in df that matches the contract column name."""
    if col in df.columns:
        return col
    col_clean = col.replace("[*]", "_0_").replace(".", "_")
    if col_clean in df.columns:
        return col_clean
    # Partial match
    for c in df.columns:
        if col in c or col_clean in c:
            return c
    return None


def sha_file(path: str) -> str:
    try:
        return hashlib.sha256(open(path, "rb").read()).hexdigest()[:16]
    except Exception:
        return "unknown"


# ── check execution ───────────────────────────────────────────────────────────

def run_clause(col: str, clause: dict, df: pd.DataFrame, prefix: str, baselines: dict) -> list[dict]:
    results = []

    actual_col = find_col(col, df)
    if actual_col is None:
        return [{
            "check_id":       f"{prefix}.{col}.exists",
            "column_name":    col,
            "check_type":     "exists",
            "status":         "ERROR",
            "actual_value":   "column not found in data",
            "expected":       "column present",
            "severity":       "CRITICAL",
            "records_failing": 0,
            "sample_failing": [],
            "message":        f"Column '{col}' not found in data. Check schema mapping.",
        }]

    series = df[actual_col]

    # ── not_null ─────────────────────────────────────────────────────────────
    if clause.get("required"):
        n = int(series.isnull().sum())
        results.append({
            "check_id":       f"{prefix}.{col}.not_null",
            "column_name":    col,
            "check_type":     "not_null",
            "status":         "FAIL" if n > 0 else "PASS",
            "actual_value":   f"null_count={n}",
            "expected":       "null_count=0",
            "severity":       "CRITICAL" if n > 0 else "LOW",
            "records_failing": n,
            "sample_failing": [],
            "message":        f"{n} null values found" if n > 0 else "OK",
        })

    dtype = str(series.dtype)
    numeric = "float" in dtype or "int" in dtype

    # ── range checks ─────────────────────────────────────────────────────────
    if numeric and "minimum" in clause:
        mn    = float(series.dropna().min()) if len(series.dropna()) else 0.0
        exp   = float(clause["minimum"])
        fail  = int((series < exp).sum())
        results.append({
            "check_id":       f"{prefix}.{col}.min",
            "column_name":    col,
            "check_type":     "range",
            "status":         "FAIL" if mn < exp else "PASS",
            "actual_value":   f"min={round(mn, 4)}",
            "expected":       f"min>={exp}",
            "severity":       "CRITICAL" if mn < exp else "LOW",
            "records_failing": fail,
            "sample_failing": [],
            "message":        f"Minimum {round(mn,4)} is below {exp}" if fail else "OK",
        })

    if numeric and "maximum" in clause:
        mx    = float(series.dropna().max()) if len(series.dropna()) else 0.0
        exp   = float(clause["maximum"])
        fail  = int((series > exp).sum())
        results.append({
            "check_id":       f"{prefix}.{col}.max",
            "column_name":    col,
            "check_type":     "range",
            "status":         "FAIL" if mx > exp else "PASS",
            "actual_value":   f"max={round(mx, 4)}",
            "expected":       f"max<={exp}",
            "severity":       "CRITICAL" if mx > exp else "LOW",
            "records_failing": fail,
            "sample_failing": [],
            "message":        f"Maximum {round(mx,4)} exceeds {exp}" if fail else "OK",
        })

    # ── statistical drift ─────────────────────────────────────────────────────
    if numeric:
        baseline = baselines.get(col, {})
        baseline_mean = baseline.get("mean", clause.get("statistics", {}).get("mean", series.mean()))
        baseline_std  = baseline.get("std",  clause.get("statistics", {}).get("std",  series.std()))
        current_mean  = float(series.dropna().mean()) if len(series.dropna()) else 0.0
        if baseline_std and baseline_std > 0:
            z = abs(current_mean - baseline_mean) / baseline_std
            status = "FAIL" if z >= 3 else ("WARNING" if z >= 2 else "PASS")
            sev    = "HIGH" if z >= 3 else ("WARNING" if z >= 2 else "LOW")
            results.append({
                "check_id":       f"{prefix}.{col}.drift",
                "column_name":    col,
                "check_type":     "statistical_drift",
                "status":         status,
                "actual_value":   f"mean={round(current_mean,4)}, z_score={round(z,2)}",
                "expected":       f"z_score<2 (baseline_mean={round(baseline_mean,4)})",
                "severity":       sev,
                "records_failing": 0,
                "sample_failing": [],
                "message":        f"Statistical drift detected: z={round(z,2)}" if z >= 2 else "OK",
            })

    # ── enum check ────────────────────────────────────────────────────────────
    if "enum" in clause:
        allowed = set(str(v) for v in clause["enum"])
        bad     = int(series.dropna().apply(lambda x: str(x) not in allowed).sum())
        results.append({
            "check_id":       f"{prefix}.{col}.enum",
            "column_name":    col,
            "check_type":     "accepted_values",
            "status":         "FAIL" if bad > 0 else "PASS",
            "actual_value":   f"invalid_count={bad}",
            "expected":       f"values in {list(clause['enum'])[:5]}",
            "severity":       "CRITICAL" if bad > 0 else "LOW",
            "records_failing": bad,
            "sample_failing": [],
            "message":        f"{bad} values outside allowed enum" if bad else "OK",
        })

    # ── uniqueness ────────────────────────────────────────────────────────────
    if clause.get("unique"):
        dups = int(series.dropna().duplicated().sum())
        results.append({
            "check_id":       f"{prefix}.{col}.unique",
            "column_name":    col,
            "check_type":     "unique",
            "status":         "FAIL" if dups > 0 else "PASS",
            "actual_value":   f"duplicates={dups}",
            "expected":       "duplicates=0",
            "severity":       "CRITICAL" if dups > 0 else "LOW",
            "records_failing": dups,
            "sample_failing": [],
            "message":        f"{dups} duplicate values found" if dups else "OK",
        })

    return results


# ── main runner ───────────────────────────────────────────────────────────────

def run_validation(contract_path: str, data_path: str, output_path: str) -> dict:
    contract = yaml.safe_load(open(contract_path))
    rows     = load_jsonl(data_path)
    df       = flatten(rows)
    cid      = contract.get("id", "unknown")

    # Load baselines
    snap_dir = Path("schema_snapshots") / cid
    baseline_path = snap_dir / "baselines.json"
    baselines = {}
    if baseline_path.exists():
        try:
            baselines = json.load(open(baseline_path))
        except Exception:
            pass

    all_results: list[dict] = []
    schema = contract.get("schema", {})

    for col, clause in schema.items():
        if isinstance(clause, dict):
            all_results.extend(run_clause(col, clause, df, cid, baselines))

    passed  = sum(1 for r in all_results if r["status"] == "PASS")
    failed  = sum(1 for r in all_results if r["status"] == "FAIL")
    warned  = sum(1 for r in all_results if r["status"] == "WARNING")
    errored = sum(1 for r in all_results if r["status"] == "ERROR")

    report = {
        "report_id":    str(uuid.uuid4()),
        "contract_id":  cid,
        "snapshot_id":  sha_file(data_path),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(all_results),
        "passed":       passed,
        "failed":       failed,
        "warned":       warned,
        "errored":      errored,
        "results":      all_results,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    # Write any FAILs to violation_log
    fails = [r for r in all_results if r["status"] in ("FAIL", "WARNING")]
    if fails:
        Path("violation_log").mkdir(exist_ok=True)
        with open("violation_log/violations.jsonl", "a") as f:
            for r in fails:
                viol = {
                    "violation_id":  str(uuid.uuid4()),
                    "check_id":      r["check_id"],
                    "detected_at":   datetime.now(timezone.utc).isoformat(),
                    "severity":      r["severity"],
                    "message":       r["message"],
                    "blame_chain":   [],
                    "blast_radius":  {
                        "affected_nodes":   [],
                        "estimated_records": r["records_failing"],
                    },
                }
                f.write(json.dumps(viol) + "\n")

    print(f"Validation complete: {passed} passed | {failed} failed | {warned} warned | {errored} errored")
    print(f"Report → {output_path}")
    return report


def main():
    ap = argparse.ArgumentParser(
        description="ValidationRunner — executes contract clauses against a data snapshot"
    )
    ap.add_argument("--contract", required=True, help="Path to contract YAML file")
    ap.add_argument("--data",     required=True, help="Path to data JSONL file")
    ap.add_argument("--output",   default="validation_reports/report.json",
                    help="Path for output JSON report")
    ap.add_argument("--mode",     choices=["AUDIT", "WARN", "ENFORCE"], default="AUDIT",
                    help="Enforcement mode: AUDIT (log only), WARN (block on CRITICAL), ENFORCE (block on CRITICAL/HIGH)")
    args = ap.parse_args()

    report = run_validation(args.contract, args.data, args.output)
    
    # Enforcement logic
    if args.mode == "WARN":
        criticals = [r for r in report["results"] if r["severity"] == "CRITICAL"]
        if criticals:
            print(f"Mode {args.mode}: Blocking on {len(criticals)} CRITICAL violations")
            exit(1)
    elif args.mode == "ENFORCE":
        high_crits = [r for r in report["results"] if r["severity"] in ("CRITICAL", "HIGH")]
        if high_crits:
            print(f"Mode {args.mode}: Blocking on {len(high_crits)} CRITICAL/HIGH violations")
            exit(1)
    # AUDIT: always pass


if __name__ == "__main__":
    main()