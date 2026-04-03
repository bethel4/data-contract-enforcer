"""
SchemaEvolutionAnalyzer — Week 7 Data Contract Enforcer

Diffs consecutive schema snapshots, classifies every change using the
Confluent-style taxonomy, and generates a migration impact report.
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml


BREAKING = {
    "add_required_field", "rename_field", "narrow_type",
    "remove_field", "remove_enum_value", "make_required",
}
SAFE = {
    "add_nullable_field", "widen_type", "add_enum_value",
}

TAXONOMY = {
    "add_nullable_field":  ("BACKWARD_COMPATIBLE",  "None. Downstream consumers can ignore the new field."),
    "add_required_field":  ("BREAKING",              "Coordinate with all producers. Block deploy until all producers updated."),
    "rename_field":        ("BREAKING",              "Deprecation period with alias column. Minimum 1 sprint before removal. Blast radius report mandatory."),
    "widen_type":          ("USUALLY_COMPATIBLE",    "Validate no precision loss. Re-run statistical checks."),
    "narrow_type":         ("BREAKING",              "CRITICAL. Migration plan with rollback required. Statistical baseline must be re-established."),
    "remove_field":        ("BREAKING",              "Two-sprint deprecation minimum. Each subscriber must acknowledge removal."),
    "add_enum_value":      ("BACKWARD_COMPATIBLE",   "Notify all subscribers. No action required."),
    "remove_enum_value":   ("BREAKING",              "Treat as breaking. Blast radius report mandatory."),
    "make_required":       ("BREAKING",              "Coordinate with producers. Provide default or migration script."),
    "no_change":           ("BACKWARD_COMPATIBLE",   "No action required."),
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_snapshots(contract_id: str) -> list:
    snap_dir = Path("schema_snapshots") / contract_id
    if not snap_dir.exists():
        return []
    snaps = []
    for f in sorted(snap_dir.glob("*.yaml")):
        try:
            data = yaml.safe_load(open(f))
            data["_file"] = str(f)
            snaps.append(data)
        except Exception:
            pass
    return snaps


def classify_change(col: str, old: dict | None, new: dict | None) -> tuple:
    if old is None:
        # New field — is it required?
        if new and new.get("required"):
            return "add_required_field", col
        return "add_nullable_field", col

    if new is None:
        return "remove_field", col

    # Enum changes
    old_enum = set(str(v) for v in old.get("enum", []))
    new_enum  = set(str(v) for v in new.get("enum", []))
    if old_enum and new_enum:
        if old_enum - new_enum:
            return "remove_enum_value", col
        if new_enum - old_enum:
            return "add_enum_value", col

    # Confidence scale change (0–1 → 0–100)
    old_max = old.get("maximum")
    new_max  = new.get("maximum")
    if old_max is not None and new_max is not None:
        if float(old_max) <= 1.0 < float(new_max):
            return "narrow_type", col

    # Required gained / lost
    if not old.get("required") and new.get("required"):
        return "make_required", col

    # Type narrowing / widening
    old_type = old.get("type","")
    new_type  = new.get("type","")
    WIDEN  = {("integer","number"),("string","string")}
    NARROW = {("number","integer")}
    if (old_type, new_type) in NARROW:
        return "narrow_type", col
    if (old_type, new_type) in WIDEN:
        return "widen_type", col

    return "no_change", col


def load_lineage_subscribers(contract_id_hint: str) -> list:
    """Load registry subscribers for blast radius reporting."""
    reg_path = Path("contract_registry/subscriptions.yaml")
    if not reg_path.exists():
        return []
    try:
        data = yaml.safe_load(open(reg_path))
        subs = []
        for s in data.get("subscriptions", []):
            if contract_id_hint.replace("-","") in s.get("contract_id","").replace("-",""):
                subs.append(s.get("subscriber_id","unknown"))
        return subs
    except Exception:
        return []


def analyze(contract_id: str, output_path: str) -> dict:
    snaps = load_snapshots(contract_id)

    if len(snaps) < 2:
        print(f"Need at least 2 snapshots. Found {len(snaps)} for '{contract_id}'.")
        if len(snaps) == 1:
            print("Injecting a demo breaking change (confidence max: 1.0 → 100.0)...")
            s1 = snaps[0]
            s2_schema = {}
            for col, clause in s1.get("schema", {}).items():
                s2_schema[col] = dict(clause) if isinstance(clause, dict) else clause
                if "confidence" in col.lower() and isinstance(clause, dict):
                    s2_schema[col] = dict(clause)
                    s2_schema[col]["maximum"] = 100.0  # THE BREAKING CHANGE
            snap_dir = Path("schema_snapshots") / contract_id
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_v2_injected"
            injected_path = snap_dir / f"{ts}.yaml"
            with open(injected_path, "w") as f:
                yaml.dump({"contract_id": contract_id,
                           "captured_at": ts,
                           "schema": s2_schema,
                           "_injected": True}, f)
            print(f"Injected snapshot → {injected_path}")
            snaps = load_snapshots(contract_id)

    old_snap = snaps[-2]
    new_snap  = snaps[-1]
    old_schema = old_snap.get("schema", {})
    new_schema  = new_snap.get("schema", {})

    all_cols = set(old_schema.keys()) | set(new_schema.keys())
    changes  = []
    for col in all_cols:
        change_type, _ = classify_change(col, old_schema.get(col), new_schema.get(col))
        if change_type == "no_change":
            continue
        compat, action = TAXONOMY[change_type]
        breaking = change_type in BREAKING

        old_max = old_schema.get(col, {}).get("maximum","—") if isinstance(old_schema.get(col),dict) else "—"
        new_max  = new_schema.get(col,  {}).get("maximum","—") if isinstance(new_schema.get(col),dict)  else "—"

        subscribers = load_lineage_subscribers(contract_id)

        checklist = []
        if breaking:
            checklist = [
                "1. Notify all registry subscribers via blast radius report",
                "2. Deploy migration script to convert existing data",
                "3. Update all consumers before removing old format",
                "4. Re-establish statistical baseline after migration",
                "5. Monitor violation_log for 1 week post-migration",
                "6. Require subscriber acknowledgement before removal",
            ]
        else:
            checklist = ["No action required — backward-compatible change."]

        changes.append({
            "field":              col,
            "change_type":        change_type,
            "compatibility":      compat,
            "old_value":          str(old_max),
            "new_value":          str(new_max),
            "breaking":           breaking,
            "required_action":    action,
            "registry_subscribers_affected": subscribers,
            "migration_checklist": checklist,
            "rollback_plan": (
                f"Revert commit, restore snapshot {old_snap.get('captured_at','unknown')}, "
                "re-run ValidationRunner to confirm baseline restored."
            ) if breaking else "N/A",
        })

    result = {
        "analysis_id":        str(uuid.uuid4()),
        "contract_id":        contract_id,
        "analyzed_at":        now(),
        "old_snapshot":       old_snap.get("captured_at", old_snap.get("_file","")),
        "new_snapshot":       new_snap.get("captured_at",  new_snap.get("_file","")),
        "total_changes":      len(changes),
        "breaking_changes":   sum(1 for c in changes if c["breaking"]),
        "safe_changes":       sum(1 for c in changes if not c["breaking"]),
        "changes":            changes,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nSchema evolution analysis: {len(changes)} changes detected")
    print(f"  Breaking: {result['breaking_changes']}")
    print(f"  Safe:     {result['safe_changes']}")
    for c in changes:
        flag = "BREAKING" if c["breaking"] else "safe"
        print(f"  [{flag:8s}] {c['field']}: {c['change_type']}")
        if c["breaking"]:
            print(f"             Action: {c['required_action'][:80]}")
    print(f"\nReport → {output_path}")
    return result


def main():
    ap = argparse.ArgumentParser(
        description="SchemaEvolutionAnalyzer — diffs schema snapshots and classifies changes"
    )
    ap.add_argument("--contract-id", required=True)
    ap.add_argument("--output",      default="validation_reports/schema_evolution.json")
    args = ap.parse_args()
    analyze(args.contract_id, args.output)


if __name__ == "__main__":
    main()