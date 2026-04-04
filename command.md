# Terminal Test Guide — Week 7 Data Contract Enforcer
# Run every command below in order. Each has expected output shown.

## SETUP (do this once)

```bash
# 1. Clone your Week 7 repo
git clone https://github.com/YOUR_USERNAME/week7-data-contract-enforcer
cd week7-data-contract-enforcer

# 2. Set up environment with uv
pip install uv
uv sync
# OR if uv not available:
pip install pandas pyyaml numpy scikit-learn reportlab python-dateutil

# 3. Add your OpenRouter key
cp .env.example .env
# Open .env and replace sk-or-v1-YOUR_KEY_HERE with your real key

# 4. Verify your data files exist
ls outputs/week3/extractions.jsonl    # should show file
ls outputs/week5/events.jsonl         # should show file
ls outputs/week4/lineage_snapshots.jsonl  # should show file

# Count records (must be 50+ for week3 and week5)
wc -l outputs/week3/extractions.jsonl   # expect: 50
wc -l outputs/week5/events.jsonl        # expect: 100
```

---

## STEP 1 — ContractGenerator (rubric: 5 pts)

```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --output generated_contracts/
```

**Expected output:**
```
Generating contract for: outputs/week3/extractions.jsonl
  LLM annotation enabled (OpenRouter)   ← only if key is set
Contract  → generated_contracts/extractions.yaml  (19 clauses)
Snapshot  → schema_snapshots/week7-extractions/20260403T...yaml
dbt file  → generated_contracts/extractions_dbt.yml
Done.
```

**Then show the contract — IMPORTANT for video:**
```bash
cat generated_contracts/extractions.yaml | grep -A 8 "confidence"
```

**Expected — point at this in your video:**
```yaml
  extracted_facts_0_confidence:
    type: number
    required: true
    minimum: 0.0
    maximum: 1.0       # ← POINT AT THIS. Say: "This is the contract clause.
    statistics:        #   Confidence MUST be float 0.0 to 1.0. Never 0-100."
      mean: 0.8872
      std: 0.0339
```

---

## STEP 2 — Inject violation and run ValidationRunner (rubric: 5 pts)

```bash
# First create the violated dataset
python inject_violation.py
```
Expected: `Created violated file: 50 records, confidence range 0.8–94.3`

```bash
# Run validation against violated data
python contracts/runner.py \
  --contract generated_contracts/extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/week3_violated.json
```
Expected: `Validation complete: 40 passed | 2 failed | 0 warned | 0 errored`

```bash
# Show the FAIL results — IMPORTANT for video
python3 -c "
import json
r = json.load(open('validation_reports/week3_violated.json'))
print(f'Total: {r[\"total_checks\"]}  Passed: {r[\"passed\"]}  FAILED: {r[\"failed\"]}')
print()
for c in r['results']:
    if c['status'] == 'FAIL':
        print(f'CHECK:    {c[\"check_id\"]}')
        print(f'STATUS:   {c[\"status\"]}')
        print(f'ACTUAL:   {c[\"actual_value\"]}')
        print(f'EXPECTED: {c[\"expected\"]}')
        print(f'SEVERITY: {c[\"severity\"]}')
        print(f'RECORDS:  {c[\"records_failing\"]} records failing')
        print()
"
```
**Expected — point at these numbers:**
```
CHECK:    week7-extractions.extracted_facts_0_confidence.max
STATUS:   FAIL
ACTUAL:   max=94.3
EXPECTED: max<=1.0
SEVERITY: CRITICAL
RECORDS:  15 records failing

CHECK:    week7-extractions.extracted_facts_0_confidence.drift
STATUS:   FAIL
ACTUAL:   mean=27.3855, z_score=690.89
EXPECTED: z_score<2 (baseline_mean=0.887232)
SEVERITY: HIGH
RECORDS:  0 records failing
```

---

## STEP 3 — ViolationAttributor / Blame Chain (rubric: 5 pts)

```bash
python contracts/attributor.py \
  --violation violation_log/violations.jsonl \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml
```

```bash
# Show the blame chain — IMPORTANT: narrate this in video
python3 -c "
import json
viols = [json.loads(l) for l in open('violation_log/violations.jsonl') if l.strip()]
v = viols[-1]
print('=== VIOLATION ===')
print(f'Check: {v[\"check_id\"]}')
print()
print('=== BLAME CHAIN ===')
for b in v.get('blame_chain',[]):
    print(f'Rank {b[\"rank\"]}: {b[\"file_path\"]}')
    print(f'  Commit: {b[\"commit_hash\"][:12]}')
    print(f'  Author: {b[\"author\"]}')
    print(f'  Message: {b[\"commit_message\"]}')
    print(f'  Confidence: {b[\"confidence_score\"]}')
print()
print('=== BLAST RADIUS ===')
br = v.get('blast_radius',{})
nodes = br.get('affected_nodes',[])
print(f'Affected nodes: {len(nodes)}')
for n in nodes[:5]:
    print(f'  {n}')
"
```

**What to SAY in video:**
> "The attributor found commit c4fbd663. This commit touched surveyor.py.
> The lineage graph shows that surveyor.py feeds into hydrologist.py,
> which writes the lineage graph, which is consumed by the attributor itself.
> 9 nodes are in the blast radius — all contaminated by this one change."

---

## STEP 4 — SchemaEvolutionAnalyzer

```bash
python contracts/schema_analyzer.py \
  --contract-id week7-extractions \
  --output validation_reports/schema_evolution.json
```

```bash
python3 -c "
import json
ev = json.load(open('validation_reports/schema_evolution.json'))
print(f'Total changes: {ev[\"total_changes\"]}')
print(f'Breaking:      {ev[\"breaking_changes\"]}')
print(f'Safe:          {ev[\"safe_changes\"]}')
for c in ev['changes']:
    flag = 'BREAKING' if c['breaking'] else 'safe'
    print(f'  [{flag}] {c[\"field\"]}: {c[\"change_type\"]}')
    if c['breaking']:
        print(f'    Action: {c[\"required_action\"][:80]}')
"
```

---

## STEP 5 — AI Extensions (rubric: 5 pts)

```bash
python contracts/ai_extensions.py \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl
```

**Show the 3 numbers clearly:**
```bash
python3 -c "
import json
ai = json.load(open('ai_metrics.json'))
print('=== AI CONTRACT EXTENSIONS ===')
print()
print(f'[1] EMBEDDING DRIFT')
print(f'    Score:     {ai[\"embedding_drift\"][\"drift_score\"]}')
print(f'    Threshold: {ai[\"embedding_drift\"][\"threshold\"]}')
print(f'    Status:    {ai[\"embedding_drift\"][\"status\"]}')
print()
print(f'[2] PROMPT INPUT VALIDATION')
print(f'    Valid:     {ai[\"prompt_input_validation\"][\"valid\"]}')
print(f'    Invalid:   {ai[\"prompt_input_validation\"][\"invalid\"]}')
print(f'    Status:    {ai[\"prompt_input_validation\"][\"status\"]}')
print()
print(f'[3] LLM OUTPUT SCHEMA')
print(f'    Total:     {ai[\"llm_output_schema\"][\"total_outputs\"]}')
print(f'    Violations:{ai[\"llm_output_schema\"][\"schema_violations\"]}')
print(f'    Rate:      {ai[\"llm_output_schema\"][\"violation_rate\"]}')
print(f'    Status:    {ai[\"llm_output_schema\"][\"status\"]}')
"
```

---

## STEP 6 — Enforcer Report (rubric: 5 pts)

```bash
python contracts/report_generator.py
```

**Show the health score and violations:**
```bash
python3 -c "
import json
r = json.load(open('enforcer_report/report_data.json'))
print(f'DATA HEALTH SCORE: {r[\"data_health_score\"]}/100')
print(f'Narrative: {r[\"health_narrative\"]}')
print()
print('TOP VIOLATIONS:')
for i,v in enumerate(r.get('top_violations',[]),1):
    print(f'{i}. [{v[\"severity\"]}] {v[\"check_id\"]}')
    print(f'   {v[\"message\"][:100]}')
print()
print('RECOMMENDED ACTIONS:')
for i,a in enumerate(r.get('recommended_actions',[]),1):
    print(f'{i}. {a[:120]}')
"
```

---

## QUICK FULL PIPELINE (1 command)

```bash
# Run everything in sequence
python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts/ && \
python contracts/runner.py --contract generated_contracts/extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/week3_clean.json && \
python inject_violation.py && \
python contracts/runner.py --contract generated_contracts/extractions.yaml --data outputs/week3/extractions_violated.jsonl --output validation_reports/week3_violated.json && \
python contracts/attributor.py && \
python contracts/schema_analyzer.py --contract-id week7-extractions --output validation_reports/schema_evolution.json && \
python contracts/ai_extensions.py && \
python contracts/report_generator.py && \
echo "=== ALL STEPS COMPLETE ==="
```