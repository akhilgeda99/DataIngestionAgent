import sys
import requests
import json

def test_analyze_quality(filename):
    url = f"http://localhost:8000/analyze/quality/{filename}"
    params = {"use_ai": True}
    response = requests.get(url, params=params)
    print(json.dumps(response.json(), indent=2))

if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else "cleaned_product_data_20250307_160452.csv"
    test_analyze_quality(filename)
