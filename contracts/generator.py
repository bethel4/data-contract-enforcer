"""
ContractGenerator — Week 7 Data Contract Enforcer
Reads JSONL source data, profiles every column structurally and statistically,
queries Week 4 lineage graph for downstream consumers, and writes:
  - Bitol-compatible YAML contract file
  - dbt schema.yml counterpart
  - Timestamped schema snapshot
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


def flatten_one_level(rows: list) -> pd.DataFrame:
    """Flatten nested JSON one level so pandas can profile it."""
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


def infer_type(series: pd.Series) -> str:
    dtype = str(series.dtype)
    if "float" in dtype:
        return "number"
    if "int" in dtype:
        return "integer"
    if "bool" in dtype:
        return "boolean"
    return "string"


def detect_format(col: str, series: pd.Series) -> str | None:
    sample = series.dropna().astype(str).head(5).tolist()
    if "id" in col.lower() and all(len(s) == 36 and s.count("-") == 4 for s in sample):
        return "uuid"
    if any(k in col.lower() for k in ["_at", "time", "date"]) and sample:
        try:
            datetime.fromisoformat(sample[0].replace("Z", "+00:00"))
            return "date-time"
        except Exception:
            pass
    if "hash" in col.lower() and sample and all(len(s) == 64 for s in sample):
        return "sha256"
    return None


def build_clause(col: str, series: pd.Series) -> dict:
    clause: dict = {
        "type": infer_type(series),
        "required": bool(series.isnull().mean() == 0),
        "description": f"Auto-profiled. Null rate: {series.isnull().mean():.1%}. "
                       f"Distinct values: {series.nunique()}.",
    }
    fmt = detect_format(col, series)
    if fmt:
        clause["format"] = fmt

    dtype = str(series.dtype)
    if "float" in dtype or "int" in dtype:
        mn, mx = float(series.min()), float(series.max())
        clause["minimum"] = mn
        clause["maximum"] = mx
        clause["statistics"] = {
            "mean":  round(float(series.mean()), 6),
            "std":   round(float(series.std()), 6),
            "p25":   round(float(series.quantile(0.25)), 6),
            "p75":   round(float(series.quantile(0.75)), 6),
        }
        # Special rule: confidence MUST be 0.0–1.0
        if "confidence" in col.lower():
            clause["minimum"] = 0.0
            clause["maximum"] = 1.0
            clause["description"] += (
                " CRITICAL: must remain float 0.0–1.0. "
                "Do NOT convert to percentage scale (0–100). "
                "A value of 0.87 means 87 percent confidence. "
                "Changing this scale is a BREAKING CHANGE that silently corrupts all downstream consumers."
            )

    # Enum detection (low-cardinality strings)
    if "float" not in dtype and "int" not in dtype and series.nunique() <= 10:
        vals = sorted(series.dropna().unique().tolist())
        clause["enum"] = [str(v) for v in vals]

    # Pattern for hash fields
    if fmt == "sha256":
        clause["pattern"] = "^[a-f0-9]{64}$"
    if fmt == "uuid":
        clause["pattern"] = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

    return clause


def load_lineage_downstream(lineage_path: str | None, source_file: str) -> list:
    """Query Week 4 lineage graph for downstream consumers of this file."""
    if not lineage_path or not Path(lineage_path).exists():
        return []
    try:
        snaps = load_jsonl(lineage_path)
        if not snaps:
            return []
        snap = snaps[-1]
        # Find nodes that READ this file
        source_node = f"file::{source_file}"
        table_node  = f"table::{source_file}"
        consumers = []
        for edge in snap.get("edges", []):
            if edge.get("source") in (source_node, table_node):
                consumers.append({
                    "id": edge["target"],
                    "relationship": edge.get("relationship", "READS"),
                    "fields_consumed": ["all"],
                })
        return consumers
    except Exception:
        return []


# ── Bitol contract builder ────────────────────────────────────────────────────

def build_contract(
    source_path: str,
    output_dir: str,
    lineage_path: str | None = None,
) -> str:
    rows = load_jsonl(source_path)
    df   = flatten_one_level(rows)
    name = Path(source_path).stem          # e.g. "extractions"
    cid  = name.replace("_", "-")          # e.g. "extractions"

    schema: dict = {}
    for col in df.columns:
        schema[col] = build_clause(col, df[col])

    downstream = load_lineage_downstream(lineage_path, source_path)

    # Build quality checks
    quality_checks = [f"row_count >= {max(1, len(rows) // 2)}"]
    if any("doc_id" in c for c in df.columns):
        quality_checks += [
            "missing_count(doc_id) = 0",
            "duplicate_count(doc_id) = 0",
        ]
    if any("confidence" in c for c in df.columns):
        quality_checks += [
            "min(confidence) >= 0.0",
            "max(confidence) <= 1.0",
            "avg(confidence) between 0.05 and 0.99",
        ]
    if any("event_id" in c for c in df.columns):
        quality_checks += [
            "missing_count(event_id) = 0",
            "duplicate_count(event_id) = 0",
        ]
    if any("sequence_number" in c for c in df.columns):
        quality_checks.append("min(sequence_number) >= 1")

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": f"week7-{cid}",
        "info": {
            "title": f"{name} — Data Contract",
            "version": "1.0.0",
            "owner": "week7-data-contract-enforcer",
            "description": (
                f"Auto-generated contract for {name}. "
                f"Profiled from {len(rows)} records. "
                f"Generated {datetime.now(timezone.utc).isoformat()}."
            ),
        },
        "servers": {
            "local": {
                "type":   "local",
                "path":   source_path,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish externally.",
            "limitations": (
                "The confidence field must remain float 0.0–1.0. "
                "Schema changes require blast-radius analysis before deployment."
            ),
        },
        "schema": schema,
        "quality": {
            "type": "SodaChecks",
            "specification": {
                f"checks for {name}": quality_checks,
            },
        },
        "lineage": {
            "upstream": [],
            "downstream": downstream,
        },
    }

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(output_dir) / f"{name}.yaml"
    with open(out_path, "w") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # ── Schema snapshot ───────────────────────────────────────────────────────
    snap_dir = Path("schema_snapshots") / f"week7-{cid}"
    snap_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    with open(snap_dir / f"{ts}.yaml", "w") as f:
        yaml.dump(
            {"contract_id": f"week7-{cid}", "captured_at": ts, "schema": schema},
            f, default_flow_style=False, sort_keys=False,
        )

    # ── dbt schema.yml ────────────────────────────────────────────────────────
    _write_dbt(name, schema, output_dir)

    print(f"Contract  → {out_path}  ({len(schema)} clauses)")
    print(f"Snapshot  → {snap_dir / (ts + '.yaml')}")
    print(f"dbt file  → {Path(output_dir) / (name + '_dbt.yml')}")
    return str(out_path)


def _write_dbt(name: str, schema: dict, output_dir: str) -> None:
    """Write a dbt schema.yml counterpart for the contract."""
    columns = []
    for col, clause in schema.items():
        col_entry: dict = {
            "name": col,
            "description": clause.get("description", ""),
            "tests": [],
        }
        if clause.get("required"):
            col_entry["tests"].append("not_null")
        if col.endswith("_id") or col == "doc_id" or col == "event_id":
            col_entry["tests"].append("unique")
        if "enum" in clause:
            col_entry["tests"].append({
                "accepted_values": {"values": clause["enum"]}
            })
        columns.append(col_entry)

    dbt = {
        "version": 2,
        "models": [
            {
                "name": name,
                "description": f"dbt-compatible contract for {name}",
                "columns": columns,
            }
        ],
    }
    dbt_path = Path(output_dir) / f"{name}_dbt.yml"
    with open(dbt_path, "w") as f:
        yaml.dump(dbt, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="ContractGenerator — auto-generates Bitol YAML contracts from JSONL data"
    )
    ap.add_argument("--source",  required=True, help="Path to source JSONL file")
    ap.add_argument("--output",  default="generated_contracts", help="Output directory for YAML files")
    ap.add_argument("--lineage", default="outputs/week4/lineage_snapshots.jsonl",
                    help="Path to Week 4 lineage snapshots JSONL")
    args = ap.parse_args()

    print(f"Generating contract for: {args.source}")
    build_contract(args.source, args.output, args.lineage)
    print("Done.")


if __name__ == "__main__":
    main()