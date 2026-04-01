import pandas as pd, yaml, json, argparse, uuid, hashlib
from pathlib import Path
from datetime import datetime, timezone

def load_jsonl(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line: rows.append(json.loads(line))
    return rows

def flatten(rows):
    flat = []
    for r in rows:
        flat_row = {}
        for k, v in r.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                flat_row[k] = v
            elif isinstance(v, list):
                flat_row[k + "_count"] = len(v)
                # Flatten first item of list if it's a dict
                if v and isinstance(v[0], dict):
                    for sk, sv in v[0].items():
                        if isinstance(sv, (str, int, float, bool)) or sv is None:
                            flat_row[k + "_0_" + sk] = sv
            elif isinstance(v, dict):
                for sk, sv in v.items():
                    if isinstance(sv, (str, int, float, bool)) or sv is None:
                        flat_row[k + "_" + sk] = sv
        flat.append(flat_row)
    return pd.DataFrame(flat)

def check_clause(col, clause, df, check_id_prefix):
    results = []
    col_clean = col.replace(".", "_").replace("[*]", "_0_")

    # Find matching column in df
    actual_col = None
    for c in df.columns:
        if col in c or col_clean in c:
            actual_col = c
            break

    if actual_col is None:
        return [{"check_id": f"{check_id_prefix}.{col}.exists", "column_name": col, "check_type": "exists",
                 "status": "ERROR", "actual_value": "column not found", "expected": "column present",
                 "severity": "CRITICAL", "records_failing": 0, "sample_failing": [], "message": f"Column {col} not found in data"}]

    series = df[actual_col]

    # Required / null check
    if clause.get("required"):
        null_count = int(series.isnull().sum())
        results.append({"check_id": f"{check_id_prefix}.{col}.not_null", "column_name": col, "check_type": "not_null",
                         "status": "FAIL" if null_count > 0 else "PASS",
                         "actual_value": f"null_count={null_count}", "expected": "null_count=0",
                         "severity": "CRITICAL" if null_count > 0 else "INFO",
                         "records_failing": null_count, "sample_failing": [], "message": f"{null_count} nulls found" if null_count > 0 else "OK"})

    # Range checks for numbers
    if "minimum" in clause and series.dtype in ["float64", "int64"]:
        min_val = float(series.min())
        expected_min = float(clause["minimum"])
        failing = int((series < expected_min).sum())
        results.append({"check_id": f"{check_id_prefix}.{col}.min", "column_name": col, "check_type": "range",
                         "status": "FAIL" if min_val < expected_min else "PASS",
                         "actual_value": f"min={round(min_val,4)}", "expected": f"min>={expected_min}",
                         "severity": "CRITICAL" if min_val < expected_min else "INFO",
                         "records_failing": failing, "sample_failing": [], "message": f"Min {round(min_val,4)} below {expected_min}" if failing else "OK"})

    if "maximum" in clause and series.dtype in ["float64", "int64"]:
        max_val = float(series.max())
        expected_max = float(clause["maximum"])
        failing = int((series > expected_max).sum())
        results.append({"check_id": f"{check_id_prefix}.{col}.max", "column_name": col, "check_type": "range",
                         "status": "FAIL" if max_val > expected_max else "PASS",
                         "actual_value": f"max={round(max_val,4)}", "expected": f"max<={expected_max}",
                         "severity": "CRITICAL" if max_val > expected_max else "INFO",
                         "records_failing": failing, "sample_failing": [], "message": f"Max {round(max_val,4)} exceeds {expected_max}" if failing else "OK"})

    # Statistical drift check — load baseline if exists
    if "statistics" in clause and series.dtype in ["float64", "int64"]:
        baseline_mean = clause["statistics"].get("mean", series.mean())
        baseline_std = clause["statistics"].get("std", series.std())
        current_mean = float(series.mean())
        if baseline_std and baseline_std > 0:
            z = abs(current_mean - baseline_mean) / baseline_std
            status = "FAIL" if z > 3 else ("WARN" if z > 2 else "PASS")
            results.append({"check_id": f"{check_id_prefix}.{col}.drift", "column_name": col, "check_type": "statistical_drift",
                             "status": status, "actual_value": f"mean={round(current_mean,4)}, z={round(z,2)}",
                             "expected": f"z-score < 2 (baseline_mean={baseline_mean})",
                             "severity": "HIGH" if status=="FAIL" else ("MEDIUM" if status=="WARN" else "INFO"),
                             "records_failing": 0, "sample_failing": [], "message": f"Statistical drift z={round(z,2)}" if z > 2 else "OK"})

    # Enum check
    if "enum" in clause:
        allowed = set(str(x) for x in clause["enum"])
        bad = series.dropna().apply(lambda x: str(x) not in allowed)
        failing = int(bad.sum())
        results.append({"check_id": f"{check_id_prefix}.{col}.enum", "column_name": col, "check_type": "accepted_values",
                         "status": "FAIL" if failing > 0 else "PASS",
                         "actual_value": f"invalid_count={failing}", "expected": f"enum={clause['enum']}",
                         "severity": "CRITICAL" if failing > 0 else "INFO",
                         "records_failing": failing, "sample_failing": [], "message": f"{failing} values not in enum" if failing else "OK"})

    return results

def run_validation(contract_path, data_path, output_path):
    contract = yaml.safe_load(open(contract_path))
    rows = load_jsonl(data_path)
    df = flatten(rows)
    contract_id = contract.get("id", "unknown")

    all_results = []
    schema = contract.get("schema", {})

    for col, clause in schema.items():
        if isinstance(clause, dict):
            results = check_clause(col, clause, df, contract_id)
            all_results.extend(results)

    passed = sum(1 for r in all_results if r["status"] == "PASS")
    failed = sum(1 for r in all_results if r["status"] == "FAIL")
    warned = sum(1 for r in all_results if r["status"] == "WARN")
    errored = sum(1 for r in all_results if r["status"] == "ERROR")

    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "snapshot_id": hashlib.sha256(open(data_path, "rb").read()).hexdigest()[:16],
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(all_results),
        "passed": passed, "failed": failed, "warned": warned, "errored": errored,
        "results": all_results
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Validation complete: {passed} passed, {failed} failed, {warned} warned, {errored} errored")
    print(f"Report → {output_path}")

    # Write FAILs to violation_log
    fails = [r for r in all_results if r["status"] in ("FAIL", "WARN")]
    if fails:
        Path("violation_log").mkdir(exist_ok=True)
        with open("violation_log/violations.jsonl", "a") as f:
            for r in fails:
                viol = {"violation_id": str(uuid.uuid4()), "check_id": r["check_id"],
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                        "severity": r["severity"], "message": r["message"],
                        "blame_chain": [], "blast_radius": {"affected_nodes": [], "estimated_records": r["records_failing"]}}
                f.write(json.dumps(viol)+"\n")
        print(f"Violations logged: {len(fails)}")
    return report

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--contract", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--output", default="validation_reports/report.json")
    args = ap.parse_args()
    run_validation(args.contract, args.data, args.output)