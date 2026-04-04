"""
SchemaEvolutionAnalyzer — Week 7 Data Contract Enforcer

Diffs two timestamped schema snapshots, classifies every change using the
Confluent-style taxonomy, and generates a full migration impact report.

Usage:
    python contracts/schema_analyzer.py --contract-id week7-extractions \
        --output validation_reports/schema_evolution.json
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
import yaml


BREAKING_CHANGES = {
    "add_required_field", "rename_field", "narrow_type",
    "remove_field", "remove_enum_value", "make_required",
}

TAXONOMY = {
    "add_nullable_field":  {"compatibility": "BACKWARD_COMPATIBLE", "confluent_mode": "BACKWARD allows",  "action": "None required. Consumers can ignore new nullable fields.", "risk": "LOW"},
    "add_required_field":  {"compatibility": "BREAKING",            "confluent_mode": "BACKWARD blocks",  "action": "Coordinate with ALL producers. Provide default value. Block deploy until all producers supply this field.", "risk": "CRITICAL"},
    "rename_field":        {"compatibility": "BREAKING",            "confluent_mode": "BACKWARD blocks",  "action": "Add alias column. Minimum 1 sprint deprecation. Notify all registry subscribers.", "risk": "CRITICAL"},
    "narrow_type":         {"compatibility": "BREAKING",            "confluent_mode": "FORWARD blocks",   "action": "CRITICAL — data meaning corrupted. Migration plan with rollback required. Re-establish statistical baseline.", "risk": "CRITICAL"},
    "remove_field":        {"compatibility": "BREAKING",            "confluent_mode": "BACKWARD blocks",  "action": "Two-sprint deprecation minimum. Each registry subscriber must acknowledge before field is dropped.", "risk": "HIGH"},
    "add_enum_value":      {"compatibility": "BACKWARD_COMPATIBLE", "confluent_mode": "BACKWARD allows",  "action": "Notify subscribers. No blocking action required.", "risk": "LOW"},
    "remove_enum_value":   {"compatibility": "BREAKING",            "confluent_mode": "BACKWARD blocks",  "action": "Treat as breaking. Blast radius report mandatory.", "risk": "HIGH"},
    "make_required":       {"compatibility": "BREAKING",            "confluent_mode": "BACKWARD blocks",  "action": "Coordinate with all producers. Provide default or migration script.", "risk": "HIGH"},
    "widen_type":          {"compatibility": "USUALLY_COMPATIBLE",  "confluent_mode": "FULL allows",      "action": "Validate no precision loss. Re-run statistical checks.", "risk": "LOW"},
    "no_change":           {"compatibility": "BACKWARD_COMPATIBLE", "confluent_mode": "All allow",        "action": "No action required.", "risk": "NONE"},
}


def now(): return datetime.now(timezone.utc).isoformat()


def load_snapshots(contract_id):
    snap_dir = Path("schema_snapshots") / contract_id
    if not snap_dir.exists():
        return []
    snaps = []
    for f in sorted(snap_dir.glob("*.yaml")):
        try:
            data = yaml.safe_load(open(f))
            data["_file"] = str(f)
            data["_filename"] = f.name
            snaps.append(data)
        except Exception as e:
            print(f"  Warning: could not load {f.name}: {e}")
    return snaps


def classify_change(col, old, new):
    if old is None:
        return "add_required_field" if (new and new.get("required")) else "add_nullable_field"
    if new is None:
        return "remove_field"

    old_enum = set(str(v) for v in (old.get("enum") or []))
    new_enum  = set(str(v) for v in (new.get("enum")  or []))
    if old_enum and new_enum:
        if old_enum - new_enum: return "remove_enum_value"
        if new_enum - old_enum: return "add_enum_value"

    old_max = old.get("maximum")
    new_max  = new.get("maximum")
    if old_max is not None and new_max is not None:
        if float(old_max) <= 1.0 < float(new_max):
            return "narrow_type"

    old_type, new_type = old.get("type",""), new.get("type","")
    if (old_type, new_type) == ("number","integer"):  return "narrow_type"
    if (old_type, new_type) == ("integer","number"):  return "widen_type"
    if not old.get("required") and new.get("required"): return "make_required"
    return "no_change"


def readable(clause):
    if clause is None: return "absent"
    parts = [clause.get("type","?")]
    if clause.get("minimum","") != "": parts.append(f"min={clause['minimum']}")
    if clause.get("maximum","") != "": parts.append(f"max={clause['maximum']}")
    parts.append("required" if clause.get("required") else "optional")
    if clause.get("enum"): parts.append(f"enum({len(clause['enum'])} values)")
    return ", ".join(str(p) for p in parts)


def registry_subscribers(contract_id_hint, breaking_field):
    reg_path = Path("contract_registry/subscriptions.yaml")
    if not reg_path.exists(): return []
    try:
        data = yaml.safe_load(open(reg_path))
        subs = []
        for sub in data.get("subscriptions", []):
            cid  = sub.get("contract_id","").replace("-","")
            hint = contract_id_hint.replace("-","")
            if hint not in cid: continue
            for bf in sub.get("breaking_fields", []):
                field_name = bf["field"] if isinstance(bf, dict) else bf
                if (breaking_field.replace("_",".") in field_name or
                    field_name.replace(".","_") in breaking_field or
                    breaking_field.split("_")[0] in field_name):
                    subs.append({"subscriber_id": sub["subscriber_id"],
                                 "validation_mode": sub.get("validation_mode","AUDIT"),
                                 "contact": sub.get("contact","")})
                    break
        return subs
    except Exception:
        return []


def migration_checklist(change_type, field, old_val, new_val):
    base = [
        f"Notify all registry subscribers via blast radius report (field: {field})",
        f"Impact review: '{old_val}' → '{new_val}'",
    ]
    if change_type == "narrow_type":
        return base + [
            "Write migration script to convert all existing data to new format",
            "Run migration in STAGING first — validate all downstream systems pass",
            "Re-establish statistical baseline: run generator.py on migrated data",
            "Update all consumer validation thresholds to match new scale",
            "Deploy to PRODUCTION — monitor violation_log for 1 week minimum",
            "Require written subscriber acknowledgement before closing migration",
        ]
    elif change_type == "add_required_field":
        return base + [
            "Add default value for the new required field in all existing records",
            "Update all producer systems to supply this field in every record",
            "Block deploy until all producers pass ValidationRunner with no ERRORs",
            "Run full pipeline end-to-end test after producers are updated",
        ]
    elif change_type == "remove_field":
        return base + [
            "Mark field as DEPRECATED in contract — do not remove yet",
            "Give all subscribers minimum 2 sprints to stop consuming this field",
            "Collect written acknowledgement from each registered subscriber",
            "Remove field only after 100% subscriber acknowledgement received",
        ]
    elif change_type == "rename_field":
        return base + [
            "Add alias column: keep both old and new name for minimum 1 sprint",
            "Notify all subscribers to migrate reads to the new field name",
            "Remove alias column only after all subscribers have migrated",
        ]
    else:
        return base + [
            "Re-run ValidationRunner to confirm all checks pass",
            "Update contract documentation to reflect new schema",
        ]


def analyze(contract_id, output_path, before_name=None, after_name=None):
    snaps = load_snapshots(contract_id)

    if len(snaps) < 2:
        print(f"ERROR: Need at least 2 snapshots. Found {len(snaps)}.")
        return {}

    before_snap = snaps[0]   # oldest
    after_snap  = snaps[-1]  # newest (includes injected breaking changes)

    if before_name:
        m = [s for s in snaps if before_name in s["_filename"]]
        if m: before_snap = m[0]
    if after_name:
        m = [s for s in snaps if after_name in s["_filename"]]
        if m: after_snap = m[0]

    before_schema = before_snap.get("schema", {})
    after_schema  = after_snap.get("schema",  {})

    print(f"\nDiffing snapshots:")
    print(f"  BEFORE: {before_snap['_filename']}")
    print(f"  AFTER:  {after_snap['_filename']}")

    all_cols = set(before_schema.keys()) | set(after_schema.keys())
    changes  = []

    for col in sorted(all_cols):
        old  = before_schema.get(col)
        new  = after_schema.get(col)
        ct   = classify_change(col, old, new)
        if ct == "no_change": continue

        info     = TAXONOMY[ct]
        breaking = ct in BREAKING_CHANGES
        subs     = registry_subscribers(contract_id, col) if breaking else []
        checklist= migration_checklist(ct, col, readable(old), readable(new))
        rollback = (
            f"Revert commit. Restore snapshot '{before_snap['_filename']}' as active schema. "
            f"Re-run ValidationRunner to confirm baseline restored."
        ) if breaking else "N/A — backward-compatible change."

        changes.append({
            "field":            col,
            "change_type":      ct,
            "compatibility":    info["compatibility"],
            "confluent_mode":   info["confluent_mode"],
            "breaking":         breaking,
            "risk":             info["risk"],
            "before":           readable(old),
            "after":            readable(new),
            "required_action":  info["action"],
            "registry_subscribers_affected": subs,
            "migration_checklist": checklist,
            "rollback_plan":    rollback,
        })

    breaking_list = [c for c in changes if c["breaking"]]
    safe_list     = [c for c in changes if not c["breaking"]]

    # ── Console output ────────────────────────────────────────────────────────
    print()
    print("=" * 56)
    print(f"SCHEMA EVOLUTION — {contract_id}")
    print("=" * 56)
    print(f"Total changes: {len(changes)}  |  BREAKING: {len(breaking_list)}  |  Safe: {len(safe_list)}")
    print()
    for c in changes:
        flag = "BREAKING" if c["breaking"] else "safe    "
        print(f"  [{flag}] {c['field']}")
        print(f"           change : {c['change_type']}")
        print(f"           before : {c['before']}")
        print(f"           after  : {c['after']}")
        if c["breaking"]:
            print(f"           risk   : {c['risk']}")
            print(f"           action : {c['required_action'][:75]}")
            subs = [s["subscriber_id"] for s in c["registry_subscribers_affected"]]
            if subs:
                print(f"           affects: {', '.join(subs)}")
            print(f"           rollback: {c['rollback_plan'][:70]}")
        print()

    risk_scores = {"CRITICAL": 3, "HIGH": 2, "LOW": 1, "NONE": 0}
    highest_risk = max((c["risk"] for c in changes), key=lambda r: risk_scores.get(r, 0)) if changes else "NONE"

    result = {
        "analysis_id":         str(uuid.uuid4()),
        "contract_id":         contract_id,
        "analyzed_at":         now(),
        "before_snapshot":     before_snap["_filename"],
        "after_snapshot":      after_snap["_filename"],
        "before_column_count": len(before_schema),
        "after_column_count":  len(after_schema),
        "total_changes":       len(changes),
        "breaking_changes":    len(breaking_list),
        "safe_changes":        len(safe_list),
        "changes":             changes,
        "migration_summary": {
            "breaking_fields":           [c["field"] for c in breaking_list],
            "highest_risk":              highest_risk,
            "total_subscribers_at_risk": len(set(
                s["subscriber_id"]
                for c in breaking_list
                for s in c["registry_subscribers_affected"]
            )),
            "immediate_action_required": len(breaking_list) > 0,
        },
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Full migration impact report → {output_path}")
    return result


def main():
    ap = argparse.ArgumentParser(
        description="SchemaEvolutionAnalyzer — diffs schema snapshots and classifies changes"
    )
    ap.add_argument("--contract-id", required=True)
    ap.add_argument("--output", default="validation_reports/schema_evolution.json")
    ap.add_argument("--before", default=None, help="Filename fragment to pin BEFORE snapshot")
    ap.add_argument("--after",  default=None, help="Filename fragment to pin AFTER snapshot")
    args = ap.parse_args()
    analyze(args.contract_id, args.output, args.before, args.after)


if __name__ == "__main__":
    main()