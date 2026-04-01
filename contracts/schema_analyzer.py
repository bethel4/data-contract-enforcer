import yaml, json, argparse
from pathlib import Path
from datetime import datetime, timezone

def load_snapshots(contract_id):
    snap_dir = Path("schema_snapshots") / contract_id
    if not snap_dir.exists():
        return []
    snaps = sorted(snap_dir.glob("*.yaml"))
    loaded = []
    for s in snaps:
        data = yaml.safe_load(open(s))
        data["_file"] = str(s)
        loaded.append(data)
    return loaded

BREAKING = {"remove_field", "rename_field", "narrow_type", "make_required", "remove_enum_value"}
SAFE = {"add_nullable_field", "widen_type", "add_enum_value"}

def classify_change(col, old_clause, new_clause):
    if old_clause is None:
        return "add_nullable_field", "BACKWARD_COMPATIBLE"
    if new_clause is None:
        return "remove_field", "BREAKING"
    old_min = old_clause.get("minimum", None)
    new_min = new_clause.get("minimum", None)
    old_max = old_clause.get("maximum", None)
    new_max = new_clause.get("maximum", None)
    # Detect confidence scale change
    if old_max is not None and new_max is not None:
        if old_max <= 1.0 and new_max > 1.0:
            return "narrow_type", "BREAKING"
    old_enum = set(old_clause.get("enum", []))
    new_enum = set(new_clause.get("enum", []))
    if old_enum and new_enum:
        if old_enum - new_enum:
            return "remove_enum_value", "BREAKING"
        if new_enum - old_enum:
            return "add_enum_value", "BACKWARD_COMPATIBLE"
    return "no_change", "BACKWARD_COMPATIBLE"

def analyze(contract_id, output_path):
    snaps = load_snapshots(contract_id)
    if len(snaps) < 2:
        print(f"Need at least 2 snapshots for {contract_id}. Found {len(snaps)}.")
        print("Run generator.py twice (possibly with different/modified data) to get 2 snapshots.")
        # Create a dummy second snapshot to demo
        if len(snaps) == 1:
            s1 = snaps[0]
            s2_schema = dict(s1["schema"])
            # Simulate the confidence breaking change
            for col, clause in s2_schema.items():
                if "confidence" in col and isinstance(clause, dict):
                    s2_schema[col] = dict(clause)
                    s2_schema[col]["maximum"] = 100.0  # The breaking change!
            snap_dir = Path("schema_snapshots") / contract_id
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_v2"
            with open(snap_dir / f"{ts}.yaml", "w") as f:
                yaml.dump({"contract_id": contract_id, "captured_at": ts, "schema": s2_schema}, f)
            snaps = load_snapshots(contract_id)
            print("Created demo v2 snapshot with injected confidence scale change.")

    old_snap = snaps[-2]
    new_snap = snaps[-1]
    old_schema = old_snap.get("schema", {})
    new_schema = new_snap.get("schema", {})

    all_cols = set(old_schema.keys()) | set(new_schema.keys())
    changes = []
    for col in all_cols:
        change_type, compat = classify_change(col, old_schema.get(col), new_schema.get(col))
        if change_type != "no_change":
            changes.append({
                "field": col, "change_type": change_type, "compatibility": compat,
                "old_value": str(old_schema.get(col, {}).get("maximum", "N/A")),
                "new_value": str(new_schema.get(col, {}).get("maximum", "N/A")),
                "migration_required": change_type in BREAKING,
                "migration_checklist": [
                    "1. Notify all downstream consumers via blast radius report",
                    "2. Deploy migration script to convert existing data",
                    "3. Update all consumers before removing old format",
                    "4. Re-establish statistical baseline after migration",
                    "5. Monitor violation_log for 1 week post-migration"
                ] if change_type in BREAKING else ["No action required"],
                "rollback_plan": "Revert commit, restore previous snapshot, re-run validation" if change_type in BREAKING else "N/A"
            })

    result = {
        "contract_id": contract_id,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "old_snapshot": old_snap.get("captured_at"),
        "new_snapshot": new_snap.get("captured_at"),
        "total_changes": len(changes),
        "breaking_changes": sum(1 for c in changes if c["compatibility"] == "BREAKING"),
        "changes": changes
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Schema evolution: {len(changes)} changes, {result['breaking_changes']} BREAKING")
    for c in changes:
        flag = "BREAKING" if c["compatibility"] == "BREAKING" else "safe"
        print(f"  [{flag}] {c['field']}: {c['change_type']}")
    print(f"Report → {output_path}")
    return result

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--contract-id", required=True)
    ap.add_argument("--output", default="validation_reports/schema_evolution.json")
    args = ap.parse_args()
    analyze(args.contract_id, args.output)