import json

# Read good data
rows = [json.loads(l) for l in open("outputs/week3/extractions.jsonl")]

# Corrupt: change confidence from 0.87 to 87 in first 10 records
for row in rows[:10]:
    for fact in row.get("extracted_facts", []):
        fact["confidence"] = fact["confidence"] * 100  # BREAKING CHANGE

# Write to a new file
with open("outputs/week3/extractions_broken.jsonl", "w") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")
print("Broken data created")