import pandas as pd
from src.analysis.data_analyzer import DataAnalyzer

# Create a simple test DataFrame
df = pd.DataFrame({
    'numeric_col': [1, 2, 3, 4, 5, None, None, None, None, None],  # 50% null
    'string_col': ['A', 'B', 'C', 'D', 'E', None, None, None, None, None],  # 50% null
    'price_col': ['$100', '$200', 'Rs300', '$400', '$500', None, None, None, None, None],  # Mixed currency
})

# Initialize analyzer
analyzer = DataAnalyzer(sample_size=10, chunk_size=5)

# Analyze data quality
results = analyzer.analyze_data_quality(df, use_sampling=False)

# Print all data quality issues (should be deduplicated)
print("\nData Quality Issues:")
for issue in results.get("data_quality_issues", []):
    print(f"- {issue['column']}: {issue['issue']} - {issue['details']}")
