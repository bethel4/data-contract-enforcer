#!/bin/bash

# Run AI extensions to generate metrics
python contracts/ai_extensions.py --extractions outputs/week3/extractions.jsonl --verdicts outputs/week2/verdicts.jsonl

# Display the metrics
python3 -c "
import json
ai = json.load(open('ai_metrics.json'))
print('[1] EMBEDDING DRIFT')
print(' Score: ', ai['embedding_drift']['drift_score'])
print(' Threshold:', ai['embedding_drift']['threshold'])
print(' Status: ', ai['embedding_drift']['status'])
print()
print('[2] PROMPT INPUT VALIDATION')
print(' Valid: ', ai['prompt_input_validation']['valid'])
print(' Invalid: ', ai['prompt_input_validation']['invalid'])
print(' Status: ', ai['prompt_input_validation']['status'])
print()
print('[3] LLM OUTPUT SCHEMA')
print(' Violations:', ai['llm_output_schema']['schema_violations'])
print(' Rate: ', ai['llm_output_schema']['violation_rate'])
print(' Status: ', ai['llm_output_schema']['status'])
"