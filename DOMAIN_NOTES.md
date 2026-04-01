# DOMAIN_NOTES.md

## Q1 — Backward-compatible vs breaking schema changes

A **backward-compatible** change means existing consumers keep working without modification.
A **breaking** change means at least one consumer must update before you deploy.

### Three backward-compatible examples from our schemas:

1. **Adding a nullable field** — Adding `notes: string | null` to Week 2 `verdict_record`.
   Existing consumers that don't read `notes` are unaffected. The field is optional.

2. **Adding an enum value** — Adding "EXTERNAL" to Week 4 `node.type` enum
   (currently FILE|TABLE|SERVICE|MODEL|PIPELINE). Consumers only reading known values
   continue to work. They simply never encounter EXTERNAL unless they process that node type.

3. **Widening a numeric type** — Changing `processing_time_ms` in Week 3 from int32 to int64.
   All existing consumers can still read the value. They gain the ability to represent larger numbers.

### Three breaking examples from our schemas:

1. **Changing confidence scale** — Week 3 `extracted_facts[*].confidence` from float 0.0–1.0
   to integer 0–100. Every consumer applying a threshold (e.g. `if confidence > 0.8`) now
   passes everything because all values are 0–100.

2. **Renaming a field** — Renaming Week 1 `intent_record.created_at` to `created_timestamp`.
   Every consumer that reads `.created_at` gets undefined/null. Silent failure.

3. **Making an optional field required** — Adding non-nullable `rubric_version` to Week 2
   `verdict_record` when existing producers don't emit it. All existing records become invalid.

---

## Q2 — Tracing the confidence 0–100 failure + Bitol contract clause

### Failure trace:

1. `src/week3/extractor.py` is updated. Developer changes:
   `confidence=score` (0.0–1.0) → `confidence=int(score*100)` (0–100).
   Unit tests pass because they only check that confidence is a number.

2. Week 3 outputs `extractions.jsonl` with values like 87, 93, 45. No type error raised.

3. Week 4 Cartographer reads `extracted_facts` and creates node metadata. It stores
   confidence as-is. Lineage edge confidence values are now 87 instead of 0.87.

4. Week 4 edge filtering (`if edge.confidence > 0.9`) now passes ALL edges.
   Lineage graph becomes saturated — everything looks high-confidence.

5. Week 7 ViolationAttributor uses this lineage graph. Every blame candidate receives
   inflated confidence. Reports are unreliable.

6. The failure propagated across 3 systems without any error being raised.

### Bitol contract clause that catches this:

```yaml
schema:
  extracted_facts:
    type: array
    items:
      confidence:
        type: number
        minimum: 0.0
        maximum: 1.0
        description: >
          CRITICAL: must remain float 0.0-1.0.
          A value of 0.87 means 87% confidence.
          DO NOT convert to percentage (0-100).
quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - min(confidence) >= 0.0
      - max(confidence) <= 1.0
      - avg(confidence) between 0.1 and 0.99
```

The statistical check `avg(confidence) between 0.1 and 0.99` catches the 0–100 change
even if the maximum:1.0 clause is removed — because the average will be ~50 not ~0.87.

---

## Q3 — How the Enforcer uses the lineage graph for blame chains

Step-by-step graph traversal:

1. **ValidationRunner emits FAIL** on `extracted_facts[*].confidence` in extractions.jsonl.
2. **Map column to node** — attributor.py looks up which lineage node has a PRODUCES edge
   pointing at `file::outputs/week3/extractions.jsonl`. Answer: `file::src/week3/extractor.py`.
3. **BFS upstream** — traverse IMPORTS and CALLS edges upstream from extractor.py.
   Collect every file that could have changed the confidence logic. Stop at external boundaries.
4. **Git log per file** — for each candidate:
   `git log --follow --since="14 days ago" --format='%H|%an|%ae|%ai|%s' -- {file}`
5. **Git blame on the specific line** — narrow down to the commit that last changed
   the confidence output line.
6. **Score candidates** — `score = 1.0 - (days_since × 0.1) - (lineage_hops × 0.2)`.
   Recent commits at fewer hops score higher.
7. **Write blame chain** — top 1–5 candidates written to `violation_log/violations.jsonl`.
8. **Compute blast radius** — BFS downstream from the failing node.
   Every reachable consumer is added to `blast_radius.affected_nodes[]`.

---

## Q4 — LangSmith trace_record data contract in Bitol YAML

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: week7-team

schema:
  id:
    type: string
    format: uuid
    required: true
    unique: true
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
  total_tokens:
    type: integer
    minimum: 0
  total_cost:
    type: number
    minimum: 0.0

quality:
  type: SodaChecks
  specification:
    checks for traces:
      # STRUCTURAL CLAUSE
      - missing_count(id) = 0
      - invalid_count(run_type) = 0
      # STATISTICAL CLAUSE
      - avg(total_cost) < 0.10
      - max(total_cost) < 5.0
      # AI-SPECIFIC CLAUSE (temporal integrity)
      - row_count(end_time <= start_time) = 0
      - row_count(total_tokens != prompt_tokens + completion_tokens) = 0
```

---

## Q5 — Why contracts go stale + how this architecture prevents it

### Why contracts go stale (most common failure mode):

Contracts are written once by hand and stored in a separate repo from the code.
Nobody owns the job of updating them. After 6 months the contract describes a system
that no longer exists. Teams stop trusting it and disable the checks.

Specific causes:
1. Manual authorship — no feedback loop when code changes
2. No baseline refresh after legitimate migrations — alerts fire constantly and get muted
3. Contracts live outside version control alongside the data they describe
4. Statistical baselines are never re-established after legitimate changes

### How this architecture prevents staleness:

1. **Auto-generation on every run** — ContractGenerator re-profiles data and regenerates YAML
   each time. The contract stays in sync with actual data shape.
2. **Timestamped snapshots** — `schema_snapshots/` stores a snapshot on every generator run.
   SchemaEvolutionAnalyzer can diff any two to detect when and what changed.
3. **Statistical baselines auto-update** — after a legitimate migration is acknowledged,
   `schema_snapshots/baselines.json` is refreshed. The system re-learns "normal."
4. **Git-anchored contracts** — ContractGenerator records the `git_commit` of each run.
   Every contract is traceable to a specific code state.

---

*Word count: ~900 words. All examples reference actual Week 1–5 schemas defined in this project.*