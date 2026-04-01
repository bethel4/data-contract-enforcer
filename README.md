# data-contract-enforcer

A utility for generating and enforcing data contracts from JSONL datasets.

## Overview

This repository profiles extraction and event data to produce machine-checkable contract artifacts. It can generate contract YAML and schema snapshots from week-specific JSONL inputs, and includes logic to detect required fields, numeric ranges, enum values, UUID/date formats, and dataset-specific invariants.

## Quick start

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Generate contracts from a JSONL source file:

   ```bash
   python contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts
   ```

3. Review generated artifacts in `generated_contracts/`.

## Repository structure

- `contracts/` – contract generation and enforcement modules
- `generated_contracts/` – generated YAML contract artifacts
- `outputs/` – raw input JSONL datasets
- `schema_snapshots/` – timestamped schema snapshots
- `validation_reports/` – generated validation output
- `enforcer_report/` – report artifacts
- `violation_log/` – logged validation failures

## Notes

- `contracts/generator.py` profiles JSONL rows and writes contract clauses with type inference, required/optional status, numeric range rules, format detection, and low-cardinality enums.
- Ignore or regenerate generated outputs as needed when datasets or schema expectations change.

## Architecture

The repository is structured as a multi-phase pipeline that:

- generates data contracts from source JSONL, including structural, statistical, lineage, and AI-assisted profiling
- validates data against contract rules and attributes violations to code and ownership
- analyzes schema evolution for compatibility and migration impact
- produces AI-enabled reports and drift metrics

### Overall pipeline

```mermaid
flowchart TD
  subgraph INPUTS["Inputs"]
    W1[outputs/week1/intent_records.jsonl]
    W3[outputs/week3/extractions.jsonl]
    W4[outputs/week4/lineage_snapshots.jsonl]
    W5[outputs/week5/events.jsonl]
    TR[outputs/traces/runs.jsonl]
    GIT[git log + git blame]
  end

  subgraph PHASE1["Phase 1 — ContractGenerator"]
    CG[generator.py]
    CG -->|structural profiling| SP[column types, null rates]
    CG -->|statistical profiling| STAT[min/max/mean/stddev]
    CG -->|lineage context| LC[downstream_consumers list]
    CG -->|LLM annotation| LA[Claude API call]
    CG --> YAML[generated_contracts/*.yaml]
    CG --> DBT[generated_contracts/*_dbt.yml]
    CG --> SNAP[schema_snapshots/timestamp.yaml]
  end

  subgraph PHASE2["Phase 2 — Validate + Attribute"]
    VR[runner.py]
    YAML --> VR
    VR --> RPT[validation_reports/*.json]
    RPT -->|FAIL results| VA[attributor.py]
    W4 --> VA
    GIT --> VA
    VA --> BL[blame chain + blast radius]
    BL --> VL[violation_log/violations.jsonl]
  end

  subgraph PHASE3["Phase 3 — SchemaEvolutionAnalyzer"]
    SNAP --> SEA[schema_analyzer.py]
    SEA --> DIFF[schema diff]
    DIFF --> COMPAT[compatibility verdict]
    COMPAT --> MIG[migration_impact report]
  end

  subgraph PHASE4["Phase 4 — AI Extensions + Report"]
    W3 --> AIE[ai_extensions.py]
    W2[outputs/week2/verdicts.jsonl] --> AIE
    AIE --> EMB[embedding drift score]
    AIE --> PIV[prompt input violations → quarantine/]
    AIE --> OVR[output schema violation rate]
    AIE --> AIM[ai_metrics.json]

    VL --> RG[report_generator.py]
    RPT --> RG
    AIM --> RG
    RG --> JSON[enforcer_report/report_data.json]
    RG --> PDF[enforcer_report/report_DATE.pdf]
  end

  W1 & W3 & W4 & W5 & TR --> CG
```

### Data lineage and validation flow

```mermaid
flowchart LR
  W1["Week 1\nintent_record\ncode_refs[].file"] -->|target_ref = code_refs.file| W2["Week 2\nverdict_record"]
  W3["Week 3\nextraction_record\ndoc_id, extracted_facts"] -->|doc_id = node, facts = metadata| W4["Week 4\nlineage_snapshot"]
  W4 -->|lineage graph| W7A["Week 7\nViolationAttributor"]
  W5["Week 5\nevent_record\npayload"] -->|payload validates against schema registry| W7B["Week 7\nContractRunner"]
  LT["LangSmith\ntrace_record"] -->|trace schema enforced| W7C["Week 7\nAI Extensions"]
  W2 -->|LLM output schema| W7C
```

### Violation attribution path

```mermaid
flowchart TD
  FAIL[ValidationRunner emits FAIL\nextracted_facts.confidence.range]
  FAIL --> MAP[Map column to lineage node\nfile::src/week3/extractor.py]
  MAP --> BFS_UP[BFS upstream\nfollow IMPORTS + CALLS edges]
  BFS_UP --> CAND[Candidate files list]
  CAND --> GL[git log --follow --since 14d\nfor each candidate]
  GL --> GB[git blame -L linestart,lineend\nfor confidence output line]
  GB --> SCORE["Score = 1.0 - (days × 0.1) - (hops × 0.2)"]
  SCORE --> RANK[Rank top 5 candidates]
  RANK --> OUT[violation_log/violations.jsonl\ncommit_hash, author, blast_radius]
```

### Schema change decision tree

```mermaid
flowchart TD
  CHG[Schema change detected] --> Q1{Is it additive?}
  Q1 -->|Yes - new nullable col or enum val| SAFE[BACKWARD_COMPATIBLE\nNo action required]
  Q1 -->|No| Q2{Type change?}
  Q2 -->|Widening int→bigint| WARN[USUALLY_COMPATIBLE\nValidate no precision loss]
  Q2 -->|Narrowing float→int or 0-1 to 0-100| BREAK1[BREAKING\nCRITICAL — blast radius report]
  Q2 -->|No type change| Q3{Rename or remove?}
  Q3 -->|Rename| BREAK2[BREAKING\nDeprecation period + alias column]
  Q3 -->|Remove| BREAK3[BREAKING\nMin 2 sprints notice + blast radius]
  Q3 -->|Add required non-null col| BREAK4[BREAKING\nCoordinate all producers first]
```
