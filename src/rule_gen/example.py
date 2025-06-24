import pandas as pd
from expectations_runner import apply_expectations

df = pd.DataFrame({
    "age": [25, 40, None],
    "salary": [50000, 120000, 20000],
    "name": ["Alice", "", "Charlie"]
})

rules = [
    "The 'age' column should not be null",
    "The 'salary' column should be between 30000 and 120000",
    "The 'name' column should not be empty"
]

results = apply_expectations(df, rules)
print(results)
