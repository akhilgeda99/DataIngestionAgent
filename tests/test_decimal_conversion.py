import pandas as pd
import numpy as np
from logging import getLogger

logger = getLogger(__name__)

def convert_to_decimal(df: pd.DataFrame, column: str, decimal_places: int = None) -> pd.Series:
    """Convert a column to decimal format, handling various number formats including currencies."""
    try:
        # Handle various input types
        series = df[column].astype(str)
        
        # Remove currency symbols, parentheses for negative numbers, and separators
        series = series.str.replace(r'[\$£€¥]', '', regex=True)  # Currency symbols
        series = series.str.replace(r'[,\s]', '', regex=True)    # Separators
        
        # Handle parentheses for negative numbers e.g., (100) -> -100
        series = series.apply(lambda x: f"-{str(x)[1:-1]}" if isinstance(x, str) and str(x).startswith('(') and str(x).endswith(')') else x)
        
        # Handle percentage values
        series = series.str.replace('%', '', regex=False)
        series = pd.to_numeric(series, errors='coerce')
        series = series.apply(lambda x: x/100 if pd.notna(x) and abs(x) > 1 and '%' in str(df[column].iloc[0]) else x)
        
        # Round if decimal places specified
        if decimal_places is not None:
            series = series.round(decimal_places)
            
        return series
    except Exception as e:
        logger.error(f"Error converting column {column} to decimal: {e}")
        return df[column]

# Create test data
data = {
    'numbers': ['123.45', '(100)', 123, '1,234.56', '$500', '75%', 'invalid'],
}
df = pd.DataFrame(data)

# Test conversion
try:
    print("Original values:")
    print(df['numbers'])
    print("\nConverted values:")
    result = convert_to_decimal(df, 'numbers', decimal_places=2)
    print(result)
except Exception as e:
    print(f"Error: {e}")
