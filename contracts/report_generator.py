"""
ReportGenerator — Week 7 Data Contract Enforcer

Reads violation_log/ + validation_reports/ + ai_metrics.json
Produces:
  enforcer_report/report_data.json   (machine-readable)
  enforcer_report/report_YYYY-MM-DD.pdf   (human-readable, auto-generated)
"""

import argparse
import glob
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
    TableStyle,
)


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


def load_all_reports(reports_dir: str) -> list:
    reports = []
    for f in Path(reports_dir).glob("*.json"):
        try:
            reports.append(json.load(open(f)))
        except Exception:
            pass
    return reports


def compute_health_score(reports: list, violations: list) -> float:
    if not reports:
        return 50.0
    total  = sum(r.get("total_checks", 0) for r in reports)
    passed = sum(r.get("passed", 0) for r in reports)
    score  = (passed / total * 100) if total else 50.0
    critical = sum(1 for v in violations if v.get("severity") == "CRITICAL")
    score    = max(0.0, score - critical * 20)
    return round(score, 1)


def top_violations(violations: list, n: int = 3) -> list:
    sev_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    sv = sorted(violations, key=lambda v: sev_order.get(v.get("severity","LOW"), 3))
    result = []
    for v in sv[:n]:
        bc  = v.get("blame_chain", [])
        br  = v.get("blast_radius", {})
        reg = br.get("registry_subscribers",[])
        result.append({
            "check_id":              v.get("check_id","unknown"),
            "severity":              v.get("severity","LOW"),
            "message":               v.get("message",""),
            "author":                bc[0]["author"] if bc else "unknown",
            "commit":                bc[0]["commit_hash"][:12] if bc else "unknown",
            "blast_nodes":           len(br.get("affected_nodes",[])),
            "registry_subscribers":  len(reg),
            "records_affected":      br.get("estimated_records", 0),
        })
    return result


def generate_report_data(
    violations_path: str,
    reports_dir:     str,
    ai_metrics_path: str,
    output_dir:      str,
) -> dict:
    violations = load_jsonl(violations_path)
    reports    = load_all_reports(reports_dir)
    ai_metrics = {}
    try:
        ai_metrics = json.load(open(ai_metrics_path))
    except Exception:
        pass

    health    = compute_health_score(reports, violations)
    top_v     = top_violations(violations)
    total_chk = sum(r.get("total_checks",0) for r in reports)
    total_pss = sum(r.get("passed",0) for r in reports)
    total_fld = sum(r.get("failed",0) for r in reports)
    total_wrn = sum(r.get("warned",0) for r in reports)

    narrative = (
        "System data health is CRITICAL — immediate action required."   if health < 50  else
        "System data health is DEGRADED — violations need attention."    if health < 75  else
        "System data health is GOOD — minor issues detected."            if health < 90  else
        "System data health is EXCELLENT — all contracts passing."
    )

    recommendations = []
    for v in top_v:
        cid = v["check_id"]
        if "confidence" in cid:
            recommendations.append(
                "Update outputs/week3/extractions.jsonl producer to output confidence "
                "as float 0.0–1.0. Re-run generator.py to refresh statistical baseline. "
                "Contract: week7-extractions clause extracted_facts_0_confidence.max."
            )
        elif "sequence" in cid:
            recommendations.append(
                "Audit Week 5 event store for sequence number gaps per aggregate_id. "
                "Run: python contracts/runner.py --contract generated_contracts/events.yaml "
                "--data outputs/week5/events.jsonl"
            )
        else:
            recommendations.append(
                f"Investigate violation '{cid}'. Review recent commits to the producing "
                f"system and run attributor.py to identify the responsible change."
            )
    if not recommendations:
        recommendations = ["No critical actions required. Continue monitoring scheduled runs."]

    # Schema evolution summary (from latest schema_evolution report if present)
    schema_changes = []
    for f in sorted(Path(reports_dir).glob("schema_evolution*.json"), reverse=True)[:1]:
        try:
            ev = json.load(open(f))
            for c in ev.get("changes", []):
                schema_changes.append({
                    "field":    c["field"],
                    "type":     c["change_type"],
                    "compat":   c["compatibility"],
                    "action":   c["required_action"][:100],
                })
        except Exception:
            pass

    report_data = {
        "report_id":          str(uuid.uuid4()),
        "generated_at":       now(),
        "generator":          "contracts/report_generator.py",
        "auto_generated":     True,
        "data_health_score":  health,
        "health_narrative":   narrative,
        "summary": {
            "total_checks": total_chk,
            "passed":       total_pss,
            "failed":       total_fld,
            "warned":       total_wrn,
            "violations":   len(violations),
        },
        "top_violations":  top_v,
        "schema_changes":  schema_changes,
        "ai_system_status": {
            "embedding_drift":         ai_metrics.get("embedding_drift", {}).get("drift_score","N/A"),
            "embedding_status":        ai_metrics.get("embedding_drift", {}).get("status","N/A"),
            "llm_violation_rate":      ai_metrics.get("llm_output_schema", {}).get("violation_rate","N/A"),
            "llm_trend":               ai_metrics.get("llm_output_schema", {}).get("trend","N/A"),
            "prompt_invalid_count":    ai_metrics.get("prompt_input_validation",{}).get("invalid","N/A"),
            "overall_ai_status":       ai_metrics.get("overall_ai_status","UNKNOWN"),
        },
        "recommended_actions": recommendations[:3],
    }

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    json_path = Path(output_dir) / "report_data.json"
    with open(json_path, "w") as f:
        json.dump(report_data, f, indent=2)
    print(f"report_data.json → {json_path}")
    return report_data


# ── PDF generation ────────────────────────────────────────────────────────────

PURPLE = colors.HexColor("#534AB7")
TEAL   = colors.HexColor("#0F6E56")
CORAL  = colors.HexColor("#993C1D")
DARK   = colors.HexColor("#1a1a2e")
MID    = colors.HexColor("#888780")
LIGHT  = colors.HexColor("#F1EFE8")
GREEN  = colors.HexColor("#3B6D11")
RED    = colors.HexColor("#A32D2D")
WHITE  = colors.white


def s(name, **kw):
    base = getSampleStyleSheet()["Normal"]
    return ParagraphStyle(name, parent=base, **kw)


def hdr(text, color, styles):
    t = Table([[Paragraph(text, s("h",fontName="Helvetica-Bold",fontSize=13,textColor=WHITE))]],
              colWidths=[17*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1),color),
        ("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),
        ("LEFTPADDING",(0,0),(-1,-1),12),
    ]))
    return t


def generate_pdf(report_data: dict, output_dir: str) -> str:
    today   = datetime.now().strftime("%Y-%m-%d")
    pdf_path = Path(output_dir) / f"report_{today}.pdf"

    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)

    BODY  = s("b", fontSize=10, leading=15, spaceAfter=6, textColor=DARK)
    H2    = s("h2", fontName="Helvetica-Bold", fontSize=13, textColor=PURPLE, spaceBefore=14, spaceAfter=6)
    H3    = s("h3", fontName="Helvetica-Bold", fontSize=11, textColor=DARK,   spaceBefore=8,  spaceAfter=4)
    CODE  = s("c", fontName="Courier", fontSize=9, leading=12, backColor=LIGHT, leftIndent=8, spaceAfter=4)
    SMALL = s("sm", fontSize=9, textColor=MID, alignment=1)

    story = []

    # Cover
    story += [Spacer(1,1*cm),
              Paragraph("Data Contract Enforcer", s("t",fontName="Helvetica-Bold",fontSize=24,textColor=DARK,spaceAfter=4)),
              Paragraph("Auto-Generated Enforcer Report  •  Machine-generated from live validation data", s("st",fontSize=12,textColor=MID,spaceAfter=16)),
              HRFlowable(width="100%",thickness=2,color=PURPLE,spaceAfter=12)]
    meta = [["Generated",report_data["generated_at"][:19]+" UTC"],
            ["Source","contracts/report_generator.py"],
            ["Data Health Score",f"{report_data['data_health_score']}/100"],
            ["Total Checks",str(report_data['summary']['total_checks'])],
            ["Violations",str(report_data['summary']['violations'])]]
    mt = Table(meta, colWidths=[4*cm,13*cm])
    mt.setStyle(TableStyle([("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),("FONTNAME",(1,0),(1,-1),"Helvetica"),
                             ("FONTSIZE",(0,0),(-1,-1),9),("TEXTCOLOR",(0,0),(0,-1),PURPLE),
                             ("ROWBACKGROUNDS",(0,0),(-1,-1),[WHITE,LIGHT]),
                             ("GRID",(0,0),(-1,-1),0.3,MID),("TOPPADDING",(0,0),(-1,-1),5),
                             ("BOTTOMPADDING",(0,0),(-1,-1),5),("LEFTPADDING",(0,0),(-1,-1),8)]))
    story += [mt, PageBreak()]

    # Section 1 — Health Score
    story += [hdr("1. Data Health Score", PURPLE, None), Spacer(1,0.3*cm)]
    score = report_data["data_health_score"]
    score_color = RED if score < 50 else CORAL if score < 75 else GREEN
    story += [Paragraph(f"<font size='32' color='#{score_color.hexval()[2:]}'><b>{score}/100</b></font>", s("sc",alignment=1,spaceAfter=6)),
              Paragraph(report_data["health_narrative"], BODY),
              Paragraph("Formula: (checks_passed / total_checks) × 100 − 20 points per CRITICAL violation.", s("fo",fontSize=9,textColor=MID)),
              Spacer(1,0.3*cm)]

    # Section 2 — Violations
    story += [hdr("2. Violations This Week", CORAL, None), Spacer(1,0.3*cm)]
    sv = report_data["summary"]
    sum_data = [["Passed","Failed","Warned","Violations"],
                [str(sv["passed"]),str(sv["failed"]),str(sv["warned"]),str(sv["violations"])]]
    st = Table(sum_data, colWidths=[4.25*cm]*4)
    st.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),DARK),("TEXTCOLOR",(0,0),(-1,0),WHITE),
                             ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),11),
                             ("FONTNAME",(0,1),(-1,1),"Helvetica-Bold"),("TEXTCOLOR",(0,1),(-1,1),DARK),
                             ("GRID",(0,0),(-1,-1),0.4,MID),("ALIGN",(0,0),(-1,-1),"CENTER"),
                             ("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8)]))
    story += [st, Spacer(1,0.3*cm)]

    if report_data["top_violations"]:
        story.append(Paragraph("Most significant violations:", H3))
        for i, v in enumerate(report_data["top_violations"], 1):
            story.append(Paragraph(
                f"{i}. <b>[{v['severity']}]</b> {v['check_id']} — {v['message'][:120]}. "
                f"Author: {v['author']}. Commit: {v['commit']}. "
                f"Registry subscribers affected: {v['registry_subscribers']}.",
                BODY))
    else:
        story.append(Paragraph("No violations detected in this run.", BODY))
    story.append(PageBreak())

    # Section 3 — Schema Changes
    story += [hdr("3. Schema Changes Detected", TEAL, None), Spacer(1,0.3*cm)]
    if report_data["schema_changes"]:
        for c in report_data["schema_changes"]:
            flag = "BREAKING" if c["compat"] == "BREAKING" else "safe"
            story.append(Paragraph(f"<b>[{flag}]</b> Field <code>{c['field']}</code>: {c['type']}. {c['action']}", BODY))
    else:
        story.append(Paragraph("No schema changes detected in the past 7 days.", BODY))
    story.append(Spacer(1,0.3*cm))

    # Section 4 — AI System Risk
    story += [hdr("4. AI System Risk Assessment", colors.HexColor("#854F0B"), None), Spacer(1,0.3*cm)]
    ai = report_data["ai_system_status"]
    ai_rows = [
        ["Metric","Value","Status"],
        ["Embedding drift score", str(ai.get("embedding_drift","N/A")), ai.get("embedding_status","N/A")],
        ["LLM output violation rate", str(ai.get("llm_violation_rate","N/A")), ai.get("llm_trend","N/A")],
        ["Prompt input invalid count", str(ai.get("prompt_invalid_count","N/A")), "—"],
        ["Overall AI status", ai.get("overall_ai_status","UNKNOWN"), "—"],
    ]
    at = Table(ai_rows, colWidths=[6*cm,5*cm,6*cm])
    at.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),DARK),("TEXTCOLOR",(0,0),(-1,0),WHITE),
                             ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),9),
                             ("FONTNAME",(0,1),(-1,-1),"Helvetica"),
                             ("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE,LIGHT]),
                             ("GRID",(0,0),(-1,-1),0.4,MID),
                             ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
                             ("LEFTPADDING",(0,0),(-1,-1),8)]))
    story += [at, Spacer(1,0.3*cm)]

    # Section 5 — Recommended Actions
    story += [hdr("5. Recommended Actions", GREEN, None), Spacer(1,0.3*cm)]
    for i, action in enumerate(report_data["recommended_actions"], 1):
        story.append(Paragraph(f"<b>Action {i}:</b> {action}", BODY))
        story.append(Spacer(1,0.2*cm))

    story += [Spacer(1,0.5*cm), HRFlowable(width="100%",thickness=1,color=MID),
              Spacer(1,0.2*cm),
              Paragraph("AUTO-GENERATED by contracts/report_generator.py — Week 7 Data Contract Enforcer",
                        s("footer",fontSize=8,textColor=MID,alignment=1))]

    doc.build(story)
    print(f"PDF → {pdf_path}")
    return str(pdf_path)


def main():
    ap = argparse.ArgumentParser(
        description="ReportGenerator — produces enforcer_report/ data and PDF"
    )
    ap.add_argument("--violations",  default="violation_log/violations.jsonl")
    ap.add_argument("--reports",     default="validation_reports")
    ap.add_argument("--ai-metrics",  default="ai_metrics.json")
    ap.add_argument("--output",      default="enforcer_report")
    args = ap.parse_args()

    data = generate_report_data(args.violations, args.reports, args.ai_metrics, args.output)
    print(f"Health score: {data['data_health_score']}/100")
    print(f"Narrative:    {data['health_narrative']}")
    generate_pdf(data, args.output)


if __name__ == "__main__":
    main()