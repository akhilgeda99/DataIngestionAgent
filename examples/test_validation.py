import requests

# Base URL
BASE_URL = 'http://127.0.0.1:8000'

def test_analyze_table():
    print("\nTesting analyze_table endpoint...")
    output_file = 'examples/car_sales_validation.json'
    url = f'{BASE_URL}/analyze/table/dbo/CAR_SALES'
    
    # Files to upload
    files = {
        'rules_file': ('car_rules.json', open('examples/car_rules.json', 'rb'), 'application/json')
    }
    
    # Add output_file parameter
    params = {
        'output_file': output_file
    }
    
    # Make the request
    response = requests.post(url, files=files, params=params)
    
    # Print the response
    print("Status Code:", response.status_code)
    print("\nResponse:")
    print(response.json())
    
    if response.status_code == 200:
        print(f"\nResults saved to: {output_file}")

if __name__ == '__main__':
    test_analyze_table()
