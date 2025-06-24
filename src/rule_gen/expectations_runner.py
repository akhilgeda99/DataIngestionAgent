import great_expectations as ge
from .rule_parser import parse_rule

def apply_expectations(df, rules):
    ge_df = ge.from_pandas(df)

    for rule in rules:
        parsed = parse_rule(rule)
        if not parsed:
            print(f"Could not parse rule: {rule}")
            continue

        col = parsed["column"]
        if parsed["type"] == "not_null":
            ge_df.expect_column_values_to_not_be_null(col)
        elif parsed["type"] == "range":
            min_val, max_val = map(int, parsed["values"])
            ge_df.expect_column_values_to_be_between(col, min_value=min_val, max_value=max_val)
        elif parsed["type"] == "not_empty":
            ge_df.expect_column_values_to_not_be_null(col)
            ge_df.expect_column_values_to_not_match_regex(col, r"^\s*$")
        elif parsed["type"] == "greater_than":
            ge_df.expect_column_values_to_be_greater_than(col, int(parsed["values"][0]))
        elif parsed["type"] == "less_than":
            ge_df.expect_column_values_to_be_less_than(col, int(parsed["values"][0]))
        else:
            print(f"Unknown expectation type for rule: {rule}")

    return ge_df.validate()
