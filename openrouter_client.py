"""
openrouter_client.py — shared OpenRouter API client for Week 7.

Reads OPENROUTER_API_KEY from .env file automatically.
Used by: generator.py (LLM column annotation)
         ai_extensions.py (real embeddings option)
         report_generator.py (plain-language summaries)

OpenRouter gives you access to Claude, GPT-4, Llama, Mistral etc.
using ONE API key and ONE endpoint — openrouter.ai/api/v1
"""

import json
import os
import urllib.request
import urllib.error
from pathlib import Path


def load_env(env_path: str = ".env") -> None:
    """Load .env file into os.environ — no python-dotenv needed."""
    p = Path(env_path)
    if not p.exists():
        return
    for line in open(p):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key not in os.environ:  # don't override real env vars
            os.environ[key] = val


# Load .env on import
load_env()


def call_openrouter(
    prompt: str,
    model: str = "anthropic/claude-3-haiku",
    system: str = "You are a precise data engineering assistant. Be concise.",
    max_tokens: int = 500,
    json_mode: bool = False,
) -> str:
    """
    Call OpenRouter API and return the response text.

    Args:
        prompt:     The user message
        model:      OpenRouter model string. Options:
                    - "anthropic/claude-3-haiku"        (fast, cheap)
                    - "anthropic/claude-3-5-sonnet"     (smart)
                    - "openai/gpt-4o-mini"              (alternative)
                    - "meta-llama/llama-3.1-8b-instruct" (free tier)
        system:     System prompt
        max_tokens: Max response tokens
        json_mode:  If True, adds instruction to respond in JSON only

    Returns:
        Response text string, or error message string.
    """
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        return "[OpenRouter API key not found. Add OPENROUTER_API_KEY=sk-or-... to your .env file]"

    if json_mode:
        system += "\nRespond ONLY with valid JSON. No markdown, no explanation, no backticks."

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": prompt},
        ],
        "max_tokens": max_tokens,
    }

    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=data,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer":  "https://github.com/week7-enforcer",
            "X-Title":       "Week7-DataContractEnforcer",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            return result["choices"][0]["message"]["content"].strip()
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return f"[OpenRouter HTTP {e.code}: {body[:200]}]"
    except Exception as e:
        return f"[OpenRouter error: {e}]"


def annotate_column(
    col_name: str,
    table_name: str,
    sample_values: list,
    col_type: str,
) -> dict:
    """
    Use LLM to annotate a column whose meaning is unclear.
    Returns dict with: description, business_rule, cross_column_relationship
    """
    prompt = f"""
Column name: {col_name}
Table: {table_name}
Type: {col_type}
Sample values: {sample_values[:5]}

Provide:
1. plain_description: one sentence describing what this column means in a financial data system
2. business_rule: a validation rule as a human-readable expression
3. cross_column_relationship: any relationship to other columns (or "none")

Respond as JSON with exactly these three keys.
"""
    response = call_openrouter(prompt, json_mode=True, max_tokens=300)
    try:
        # Strip markdown fences if present
        clean = response.replace("```json", "").replace("```", "").strip()
        return json.loads(clean)
    except Exception:
        return {
            "plain_description": f"Column {col_name} from {table_name}",
            "business_rule": "No rule inferred",
            "cross_column_relationship": "none",
        }


def plain_language_violation(check_id: str, actual: str, expected: str, severity: str) -> str:
    """
    Convert a technical violation into plain English for the Enforcer Report.
    """
    prompt = f"""
A data contract violation was detected:
- Check: {check_id}
- Actual value: {actual}
- Expected: {expected}
- Severity: {severity}

Write ONE sentence in plain English that a product manager can understand.
Explain: which system has the problem, what the problem is, and why it matters to the business.
Do not use technical jargon like z-score, stddev, or JSONL.
"""
    return call_openrouter(prompt, max_tokens=150)


if __name__ == "__main__":
    print("Testing OpenRouter connection...")
    load_env()
    key = os.environ.get("OPENROUTER_API_KEY", "")
    if not key:
        print("No OPENROUTER_API_KEY in .env — add it first")
    else:
        result = call_openrouter("Say 'OpenRouter connection successful' and nothing else.")
        print(f"Response: {result}")