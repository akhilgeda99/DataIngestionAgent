"""Test script for file analysis functionality."""
import sys
from pathlib import Path
import logging
import pandas as pd
import json

# Add src directory to Python path
src_dir = str(Path(__file__).parent.parent / 'src')
if src_dir not in sys.path:
    sys.path.append(src_dir)

from analysis.data_analyzer import DataAnalyzer

def create_sample_data():
    """Create a sample dataset for testing."""
    data = {
        'ID': range(1, 101),
        'Name': [f'Item {i}' for i in range(1, 101)],
        'Category': ['A', 'B', 'C', 'D'] * 25,
        'Value': [10 * i + 0.5 for i in range(1, 101)],
        'InStock': [True, False] * 50,
        'LastUpdated': pd.date_range(start='2024-01-01', periods=100, freq='D'),
        'Description': [None if i % 10 == 0 else f'Description for item {i}' for i in range(1, 101)]
    }
    return pd.DataFrame(data)

def test_file_analysis():
    """Test file analysis functionality."""
    try:
        print("\n=== Testing File Analysis ===")
        
        # Create sample data
        print("\nCreating sample data...")
        df = create_sample_data()
        
        # Save to different file formats for testing
        test_dir = Path(__file__).parent / 'test_data'
        test_dir.mkdir(exist_ok=True)
        
        # Save as CSV
        csv_path = test_dir / 'sample_data.csv'
        df.to_csv(csv_path, index=False)
        print(f"✓ Created CSV file: {csv_path}")
        
        # Save as Excel
        excel_path = test_dir / 'sample_data.xlsx'
        df.to_excel(excel_path, index=False)
        print(f"✓ Created Excel file: {excel_path}")
        
        # Initialize analyzer
        analyzer = DataAnalyzer()
        
        # Test CSV analysis
        print("\n=== Analyzing CSV File ===")
        csv_analysis = analyzer.analyze_data_quality(df)
        print("\nAnalysis Results:")
        print(json.dumps(csv_analysis, indent=2))
        
        print("\nKey Findings:")
        print(f"- Total rows: {csv_analysis['total_rows']}")
        print(f"- Total columns: {csv_analysis['total_columns']}")
        print("\nMissing Values:")
        for col, count in csv_analysis['missing_values_per_column'].items():
            if count > 0:
                print(f"- {col}: {count} missing values")
        
        print("\nData Types:")
        for col, dtype in csv_analysis['data_types'].items():
            print(f"- {col}: {dtype}")
        
        print("\nNumerical Statistics:")
        for col, stats in csv_analysis.get('numerical_statistics', {}).items():
            print(f"\n{col}:")
            print(f"  Mean: {stats['mean']:.2f}")
            print(f"  Min: {stats['min']:.2f}")
            print(f"  Max: {stats['max']:.2f}")
            print(f"  Standard Deviation: {stats['std']:.2f}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        logging.exception("Error in file analysis test")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    test_file_analysis()
