import sys
import requests
import json

def test_analyze_table(schema: str, table_name: str):
    """Test the table analysis endpoint with AI insights."""
    url = f"http://localhost:8000/analyze/table/{schema}/{table_name}"
    params = {"use_ai": True}
    
    print(f"\nAnalyzing table: {schema}.{table_name}")
    response = requests.get(url, params=params)
    print(json.dumps(response.json(), indent=2))

if __name__ == "__main__":
    # Default values
    schema = "dbo"
    table_name = "Employees"
    
    # Override with command line arguments if provided
    if len(sys.argv) > 2:
        schema = sys.argv[1]
        table_name = sys.argv[2]
    elif len(sys.argv) > 1:
        table_name = sys.argv[1]
        
    test_analyze_table(schema, table_name)
