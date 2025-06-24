import re

def parse_rule(rule: str):
    rule = rule.lower()
    patterns = [
        {
            "pattern": r"the '(.+)' column should not be null",
            "type": "not_null"
        },
        {
            "pattern": r"the '(.+)' column should be between (\d+) and (\d+)",
            "type": "range"
        },
        {
            "pattern": r"the '(.+)' column should not be empty",
            "type": "not_empty"
        },
        {
            "pattern": r"the '(.+)' column should be greater than (\d+)",
            "type": "greater_than"
        },
        {
            "pattern": r"the '(.+)' column should be less than (\d+)",
            "type": "less_than"
        }
    ]
    for p in patterns:
        match = re.search(p["pattern"], rule)
        if match:
            return {"type": p["type"], "column": match.group(1), "values": match.groups()[1:]}
    return None
