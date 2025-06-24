import sys
import pandas as pd
from src.analysis.transformer_analyzer import transformer_analyzer

def test_transformer_analysis(filename: str):
    """Test the transformer-based analysis on a file."""
    try:
        # Read the file
        df = pd.read_csv(filename)
        
        # Run analysis
        context = {
            "source": "file",
            "filename": filename,
            "file_type": filename.split('.')[-1]
        }
        
        insights = transformer_analyzer.analyze_dataframe(df, context=context)
        
        # Print results
        print("\nTransformer Analysis Results:")
        print("=" * 50)
        
        print("\nKey Findings:")
        for finding in insights["key_findings"]:
            print(f"- {finding}")
            
        print("\nData Quality:")
        for aspect, assessment in insights["data_quality"].items():
            print(f"\n{aspect.title()}:")
            print(assessment)
            
        print("\nRecommendations:")
        for rec in insights["recommendations"]:
            print(f"- {rec}")
            
        print("\nPotential Use Cases:")
        for use_case in insights["potential_use_cases"]:
            print(f"- {use_case}")
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_transformer.py <filename>", file=sys.stderr)
        sys.exit(1)
        
    test_transformer_analysis(sys.argv[1])
