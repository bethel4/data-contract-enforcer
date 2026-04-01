import pandas as pd, yaml, json, argparse, hashlib, uuid
from pathlib import Path
from datetime import datetime, timezone

def load_jsonl(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def flatten_for_profiling(rows):
    """Flatten nested JSON one level for pandas profiling"""
    flat = []
    for r in rows:
        flat_row = {}
        for k, v in r.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                flat_row[k] = v
            elif isinstance(v, list):
                flat_row[k + "_count"] = len(v)
            elif isinstance(v, dict):
                for sk, sv in v.items():
                    if isinstance(sv, (str, int, float, bool)) or sv is None:
                        flat_row[k + "_" + sk] = sv
        flat.append(flat_row)
    return pd.DataFrame(flat)

def infer_type(series):
    dtype = str(series.dtype)
    if "float" in dtype: return "number"
    if "int" in dtype: return "integer"
    if "bool" in dtype: return "boolean"
    return "string"

def build_clause(col, series):
    clause = {
        "type": infer_type(series),
        "required": bool(series.isnull().mean() == 0),
        "description": f"Auto-profiled. Null rate: {series.isnull().mean():.1%}. Cardinality: {series.nunique()}"
    }
    dtype = str(series.dtype)
    if "float" in dtype or "int" in dtype:
        clause["minimum"] = float(series.min())
        clause["maximum"] = float(series.max())
        clause["statistics"] = {
            "mean": round(float(series.mean()), 4),
            "std": round(float(series.std()), 4),
            "p25": round(float(series.quantile(0.25)), 4),
            "p75": round(float(series.quantile(0.75)), 4)
        }
        # Special rule: confidence field must be 0.0–1.0
        if "confidence" in col.lower():
            clause["minimum"] = 0.0
            clause["maximum"] = 1.0
            clause["description"] += " CRITICAL: must remain float 0.0-1.0. Do NOT convert to percentage."
    if "float" not in dtype and "int" not in dtype and series.nunique() <= 10:
        clause["enum"] = sorted(series.dropna().unique().tolist())
    return clause

def generate_contract(source_path, output_dir, lineage_path=None):
    rows = load_jsonl(source_path)
    df = flatten_for_profiling(rows)
    name = Path(source_path).stem
    contract_id = name.replace("_", "-")

    # Build schema clauses
    schema = {}
    for col in df.columns:
        schema[col] = build_clause(col, df[col])

    # Load downstream consumers from lineage if available
    downstream = []
    if lineage_path and Path(lineage_path).exists():
        try:
            snaps = load_jsonl(lineage_path)
            if snaps:
                snap = snaps[-1]
                file_node = f"file::outputs/{Path(source_path).parent.name}/{Path(source_path).name}"
                for edge in snap.get("edges", []):
                    if edge.get("source") == file_node:
                        downstream.append({"id": edge["target"], "fields_consumed": ["all"]})
        except Exception:
            pass

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": f"{name} — Data Contract",
            "version": "1.0.0",
            "owner": "week7-team",
            "description": f"Auto-generated contract for {name}. {len(rows)} records profiled."
        },
        "servers": {"local": {"type": "local", "path": str(source_path), "format": "jsonl"}},
        "terms": {"usage": "Internal inter-system contract.", "limitations": "Do not change column types without updating this contract."},
        "schema": schema,
        "quality": {
            "type": "SodaChecks",
            "specification": {f"checks for {name}": [
                f"row_count >= 1",
                "missing_count(doc_id) = 0" if "doc_id" in df.columns else "row_count >= 1",
                "min(confidence) >= 0.0" if any("confidence" in c for c in df.columns) else "row_count >= 1",
                "max(confidence) <= 1.0" if any("confidence" in c for c in df.columns) else "row_count >= 1",
            ]}
        },
        "lineage": {
            "upstream": [],
            "downstream": downstream
        }
    }

    out_path = Path(output_dir) / f"{name}.yaml"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False)

    # Save schema snapshot
    snap_dir = Path("schema_snapshots") / contract_id
    snap_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    snap_path = snap_dir / f"{ts}.yaml"
    with open(snap_path, "w") as f:
        yaml.dump({"contract_id": contract_id, "captured_at": ts, "schema": schema}, f, default_flow_style=False)

    print(f"Contract → {out_path}  ({len(schema)} clauses)")
    print(f"Snapshot → {snap_path}")
    return out_path

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--output", default="generated_contracts")
    ap.add_argument("--lineage", default="outputs/week4/lineage_snapshots.jsonl")
    args = ap.parse_args()
    generate_contract(args.source, args.output, args.lineage)