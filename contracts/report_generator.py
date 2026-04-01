import json, argparse
from pathlib import Path
from datetime import datetime, timezone

def load_jsonl(path):
    rows = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line: rows.append(json.loads(line))
    except Exception: pass
    return rows

def load_all_reports(reports_dir):
    reports = []
    for f in Path(reports_dir).glob("*.json"):
        try: reports.append(json.load(open(f)))
        except Exception: pass
    return reports

def compute_health_score(reports, violations):
    if not reports: return 50
    total = sum(r.get("total_checks", 0) for r in reports)
    passed = sum(r.get("passed", 0) for r in reports)
    score = (passed / total * 100) if total else 50
    critical = sum(1 for v in violations if v.get("severity") == "CRITICAL")
    score = max(0, score - (critical * 20))
    return round(score, 1)

def top_violations(violations, n=3):
    sev_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    sorted_v = sorted(violations, key=lambda v: sev_order.get(v.get("severity","LOW"), 3))
    top = []
    for v in sorted_v[:n]:
        bc = v.get("blame_chain", [])
        author = bc[0]["author"] if bc else "unknown"
        br = v.get("blast_radius", {})
        top.append({
            "check_id": v.get("check_id", "unknown"),
            "severity": v.get("severity", "LOW"),
            "message": v.get("message", ""),
            "author": author,
            "blast_radius_nodes": len(br.get("affected_nodes", [])),
            "records_affected": br.get("estimated_records", 0)
        })
    return top

def generate_report(violations_path, reports_dir, ai_metrics_path, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    violations = load_jsonl(violations_path)
    reports = load_all_reports(reports_dir)
    ai_metrics = {}
    try: ai_metrics = json.load(open(ai_metrics_path))
    except Exception: pass

    health_score = compute_health_score(reports, violations)
    top_v = top_violations(violations)
    total_checks = sum(r.get("total_checks",0) for r in reports)
    total_passed = sum(r.get("passed",0) for r in reports)
    total_failed = sum(r.get("failed",0) for r in reports)

    recommendations = []
    for v in top_v:
        if "confidence" in v["check_id"]:
            recommendations.append("Update src/week3/extractor.py: change confidence output back to float 0.0-1.0 (not percentage). Re-run generator.py to refresh contract baseline.")
        elif "null" in v["check_id"].lower():
            recommendations.append(f"Fix missing required fields in {v['check_id'].split('.')[0]} — add null checks before writing JSONL output.")
        else:
            recommendations.append(f"Investigate violation in {v['check_id']} — review recent commits to the producing system.")
    if not recommendations:
        recommendations = ["No critical actions required. Continue monitoring."]

    report_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_health_score": health_score,
        "health_narrative": (
            "System data health is CRITICAL — immediate action required." if health_score < 50 else
            "System data health is DEGRADED — violations need attention." if health_score < 75 else
            "System data health is GOOD — minor issues detected." if health_score < 90 else
            "System data health is EXCELLENT — all contracts passing."
        ),
        "summary": {"total_checks": total_checks, "passed": total_passed, "failed": total_failed, "violations": len(violations)},
        "top_violations": top_v,
        "ai_system_status": {
            "embedding_drift": ai_metrics.get("embedding_drift", {}).get("drift_score", "N/A"),
            "embedding_status": ai_metrics.get("embedding_drift", {}).get("status", "N/A"),
            "llm_violation_rate": ai_metrics.get("llm_output_schema", {}).get("violation_rate", "N/A"),
            "llm_trend": ai_metrics.get("llm_output_schema", {}).get("trend", "N/A"),
            "overall_ai_status": ai_metrics.get("overall_ai_status", "UNKNOWN")
        },
        "recommended_actions": recommendations[:3]
    }

    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    json_path = Path(output_dir) / "report_data.json"
    with open(json_path, "w") as f:
        json.dump(report_data, f, indent=2)
    print(f"Report data → {json_path}")
    print(f"Data Health Score: {health_score}/100")
    print(f"Narrative: {report_data['health_narrative']}")

    # Generate simple text report (PDF requires reportlab — optional)
    txt_path = Path(output_dir) / f"report_{today}.txt"
    lines = [
        "=" * 60,
        "DATA CONTRACT ENFORCER — WEEKLY REPORT",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 60,
        f"\nDATA HEALTH SCORE: {health_score}/100",
        f"{report_data['health_narrative']}",
        f"\nSUMMARY: {total_checks} checks run. {total_passed} passed. {total_failed} failed.",
        f"Violations logged: {len(violations)}",
        "\nTOP VIOLATIONS:",
    ]
    for i, v in enumerate(top_v, 1):
        lines.append(f"  {i}. [{v['severity']}] {v['check_id']}")
        lines.append(f"     {v['message']}")
        lines.append(f"     Author: {v['author']} | Records affected: {v['records_affected']}")
    lines += ["\nAI SYSTEM STATUS:",
              f"  Embedding drift: {report_data['ai_system_status']['embedding_drift']} ({report_data['ai_system_status']['embedding_status']})",
              f"  LLM violation rate: {report_data['ai_system_status']['llm_violation_rate']} (trend: {report_data['ai_system_status']['llm_trend']})",
              "\nRECOMMENDED ACTIONS:"]
    for i, a in enumerate(recommendations[:3], 1):
        lines.append(f"  {i}. {a}")
    lines.append("\n" + "=" * 60)

    with open(txt_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Text report → {txt_path}")
    return report_data

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--violations", default="violation_log/violations.jsonl")
    ap.add_argument("--reports", default="validation_reports")
    ap.add_argument("--ai-metrics", default="ai_metrics.json")
    ap.add_argument("--output", default="enforcer_report")
    args = ap.parse_args()
    generate_report(args.violations, args.reports, args.ai_metrics, args.output)