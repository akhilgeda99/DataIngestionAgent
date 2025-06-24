"""Data validation module using Great Expectations."""

import json
import pandas as pd
import polars as pl
import numpy as np
import re
import great_expectations as ge
from great_expectations.core import ExpectationConfiguration
from great_expectations.core import ExpectationSuite
from logging import getLogger
from typing import Union
from analysis.utils.data_transformers import to_polars

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

logger = getLogger(__name__)

# Function to create an in-memory Expectation Suite
def create_expectation_suite(rules: list) -> ExpectationSuite:
    suite = ExpectationSuite(expectation_suite_name="dynamic")
    
    # Loop through rules and add to suite
    for rule in rules:
        exp_type = rule["expectation_type"]
        kwargs = rule["kwargs"]
        meta = rule.get("meta", {})

        # Handle expressions in expect_column_pair_values_to_be_equal
        if exp_type == "expect_column_pair_values_to_be_equal":
            col_b = kwargs["column_B"]
            if re.search(r"[\*\+\-/]", col_b) and not col_b.isidentifier():
                rule_id = rule.get("rule_id", "expr_col")
                expr_col_name = f"__expr_col_{rule_id}"
                kwargs["column_B"] = expr_col_name
                meta["expression_column"] = col_b
                meta["generated_column_name"] = expr_col_name

        expectation_config = ExpectationConfiguration(
            expectation_type=exp_type,
            kwargs=kwargs,
            meta=meta
        )

        suite.add_expectation(expectation_config)

    return suite

# Function to validate data based on the uploaded file and rules
def escape_column_name(column: str) -> str:
    """Escape column names that contain spaces or special characters."""
    if ' ' in column or any(c in column for c in '+-/*()[]'):
        return f'`{column}`'  # Wrap in backticks for pandas eval
    return column

def preprocess_expression(df: pd.DataFrame, expression: str, decimal_places: int = None, date_format: str = None, column_mapping: dict = None) -> tuple:
    """Convert columns in expression to appropriate types and prepare for evaluation.
    
    Args:
        df: DataFrame containing the data
        expression: Expression to evaluate
        decimal_places: Number of decimal places to round to (optional)
        date_format: Optional format string for datetime parsing
        column_mapping: Optional dictionary to map column names in the expression
        
    Returns:
        tuple: (modified expression, list of temporary columns created)
    """
    # Apply column mapping if provided
    modified_expression = expression
    if column_mapping:
        for old_col, new_col in column_mapping.items():
            modified_expression = re.sub(r'\b' + re.escape(old_col) + r'\b', new_col, modified_expression)
    
    # Find potential column names in the expression
    potential_columns = re.findall(r'\b[A-Za-z_][A-Za-z0-9_\s]*\b', modified_expression)
    temp_columns = []
    modified_expr = modified_expression
    
    # Process each potential column
    for col in potential_columns:
        col = col.strip()
        if col in df.columns:
            # Detect column type
            col_info = detect_column_type(df, col)
            base_type = col_info['base_type']
            specific_type = col_info['specific_type']
            sample_format = col_info['sample_format']
            
            # Always create a numeric version for calculations
            # Replace spaces with underscores in temp column name
            temp_col = f"__numeric_{col.replace(' ', '_')}"
            
            if base_type == 'datetime':
                # Convert datetime to Unix timestamp for calculations
                dt_series = convert_to_datetime(df, col, date_format or sample_format)
                df[temp_col] = dt_series.astype(np.int64) // 10**9  # Convert to Unix timestamp
            elif base_type == 'numeric':
                if specific_type == 'integer':
                    df[temp_col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
                else:
                    df[temp_col] = convert_to_decimal(df, col, decimal_places)
            elif base_type == 'boolean':
                df[temp_col] = df[col].map(lambda x: 1 if str(x).lower() in {'true', 'yes', 'y', '1', 't'} else 0)
            else:
                # For string/categorical, try numeric conversion
                df[temp_col] = convert_to_decimal(df, col, decimal_places)
                if df[temp_col].isna().all():
                    logger.warning(f"Column '{col}' could not be converted to numeric type")
                    df[temp_col] = 0  # Default to 0 if conversion fails
            
            # Ensure the column is numeric
            if not pd.api.types.is_numeric_dtype(df[temp_col]):
                df[temp_col] = pd.to_numeric(df[temp_col], errors='coerce').fillna(0)
            
            # Replace in expression with escaped name
            modified_expr = re.sub(r'\b' + re.escape(col) + r'\b', escape_column_name(temp_col), modified_expr)
            temp_columns.append(temp_col)
    
    return modified_expr, temp_columns

def evaluate_expression(df: pd.DataFrame, expression: str, decimal_places: int = None) -> pd.Series:
    """Evaluate a pandas expression and return the result.
    
    Args:
        df: DataFrame containing the data
        expression: Expression to evaluate
        decimal_places: Number of decimal places to round to (optional)
        
    Returns:
        pd.Series: Result of the expression evaluation
    """
    try:
        # Clean up the expression
        expression = expression.replace("=", "==")  # Replace single = with ==
        # Do not normalize whitespace to preserve spaces in column names
        
        # Convert columns to decimal and get modified expression
        processed_expr, temp_columns = preprocess_expression(df, expression, decimal_places)
        
        # Ensure all temporary columns are numeric
        for col in temp_columns:
            if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        logger.info(f"Evaluating expression: {processed_expr}")
        result = pd.to_numeric(df.eval(processed_expr), errors='coerce')
        
        # Round final result if decimal places specified
        if decimal_places is not None:
            result = result.round(decimal_places)
            
        return result
    except Exception as e:
        logger.error(f"Error evaluating expression '{expression}': {e}")
        raise ValueError(f"Invalid expression: {expression}. Error: {e}")

def convert_to_decimal(df: pd.DataFrame, column: str, decimal_places: int = None) -> pd.Series:
    """Convert a column to decimal format, handling various number formats including currencies.
    
    Args:
        df: DataFrame containing the data
        column: Column name to convert
        decimal_places: Number of decimal places to round to (optional)
        
    Returns:
        pd.Series: Converted numeric series
    """
    try:
        # Handle various input types
        series = df[column].astype(str)
        
        # First check for parentheses and mark as negative
        is_negative = series.str.startswith('(') & series.str.endswith(')')
        # Remove parentheses if present
        series = series.str.replace(r'^\((.*?)\)$', r'\1', regex=True)
        
        # Remove currency symbols and separators
        series = series.str.replace(r'[\$£€¥]', '', regex=True)  # Currency symbols
        series = series.str.replace(r'[,\s]', '', regex=True)    # Separators
        
        # Handle percentage values
        has_percent = series.str.contains('%', regex=False)
        series = series.str.replace('%', '', regex=False)
        
        # Convert to numeric
        series = pd.to_numeric(series, errors='coerce')
        
        # Apply negative sign where needed
        series = series.where(~is_negative, -series)
        
        # Handle percentages
        series = series.where(~has_percent, series/100)
        
        # Round if decimal places specified
        if decimal_places is not None:
            series = series.round(decimal_places)
            
        return series
    except Exception as e:
        logger.error(f"Error converting column {column} to decimal: {e}")
        return df[column]

def convert_to_datetime(df: pd.DataFrame, column: str, format: str = None) -> pd.Series:
    """Convert a column to datetime format with intelligent format detection.
    
    Args:
        df: DataFrame containing the data
        column: Column name to convert
        format: Optional datetime format string (e.g., '%Y-%m-%d')
        
    Returns:
        pd.Series: Converted datetime series
    """
    try:
        series = df[column]
        
        # If format is provided, try it first
        if format:
            result = pd.to_datetime(series, format=format, errors='coerce')
            # If too many NaT values, try flexible parsing
            if result.isna().mean() > 0.5:  # If more than 50% conversion failed
                logger.warning(f"Specified format '{format}' failed for many values in {column}, trying flexible parsing")
                result = pd.to_datetime(series, errors='coerce')
            return result
        
        # Try common preprocessing steps
        if series.dtype == object:
            # Remove any timezone indicators if they cause problems
            series = series.str.replace('UTC', '').str.replace('GMT', '')
            # Handle Excel numeric dates
            if series.str.match(r'^\d{5,6}(\.\d+)?$').any():
                try:
                    return pd.to_datetime(pd.to_numeric(series, errors='coerce'), unit='D', origin='1899-12-30')
                except:
                    pass
        
        return pd.to_datetime(series, errors='coerce')
    except Exception as e:
        logger.error(f"Error converting column {column} to datetime: {e}")
        return df[column]

def detect_column_type(df: pd.DataFrame, column: str) -> dict:
    """Detect the detailed type of data in a column based on sample values.
    
    Args:
        df: DataFrame containing the data
        column: Name of the column to analyze
        
    Returns:
        dict: Dictionary containing:
            - base_type: Basic type (numeric, datetime, boolean, categorical, string)
            - specific_type: More specific type information
            - sample_format: Example format for datetime or numbers
            - unique_ratio: Ratio of unique values (for categorical detection)
    """
    # Get non-null samples for analysis
    samples = df[column].dropna()
    total_samples = len(samples)
    if total_samples == 0:
        return {
            'base_type': 'unknown',
            'specific_type': 'unknown',
            'sample_format': None,
            'unique_ratio': 0
        }
    
    # Check if already datetime
    if pd.api.types.is_datetime64_any_dtype(df[column]):
        sample_date = samples.iloc[0]
        return {
            'base_type': 'datetime',
            'specific_type': 'datetime64',
            'sample_format': sample_date.strftime('%Y-%m-%d %H:%M:%S'),
            'unique_ratio': len(samples.unique()) / total_samples
        }
    
    # Check for boolean
    bool_values = {'true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '1', '0'}
    sample_lower = samples.astype(str).str.lower()
    if all(val in bool_values for val in sample_lower.unique()):
        return {
            'base_type': 'boolean',
            'specific_type': 'boolean',
            'sample_format': None,
            'unique_ratio': len(samples.unique()) / total_samples
        }
    
    # Try converting to datetime
    try:
        datetime_series = pd.to_datetime(samples, errors='raise')
        sample_date = datetime_series.iloc[0]
        return {
            'base_type': 'datetime',
            'specific_type': 'datetime',
            'sample_format': sample_date.strftime('%Y-%m-%d %H:%M:%S'),
            'unique_ratio': len(datetime_series.unique()) / total_samples
        }
    except:
        pass
    
    # Try converting to numeric
    numeric_series = pd.to_numeric(samples, errors='coerce')
    if numeric_series.notna().all():
        # Check if all numbers are integers
        if (numeric_series == numeric_series.astype(int)).all():
            specific_type = 'integer'
            sample_format = f'{numeric_series.iloc[0]:.0f}'
        else:
            specific_type = 'float'
            # Detect decimal places
            max_decimals = max(str(num)[::-1].find('.') for num in numeric_series if '.' in str(num))
            sample_format = f'{numeric_series.iloc[0]:.{max_decimals}f}'
        
        return {
            'base_type': 'numeric',
            'specific_type': specific_type,
            'sample_format': sample_format,
            'unique_ratio': len(numeric_series.unique()) / total_samples
        }
    
    # Check for categorical (if relatively few unique values)
    unique_ratio = len(samples.unique()) / total_samples
    if unique_ratio < 0.1 and len(samples.unique()) < 50:  # Less than 10% unique values and fewer than 50 categories
        return {
            'base_type': 'categorical',
            'specific_type': 'categorical',
            'sample_format': None,
            'unique_ratio': unique_ratio
        }
    
    # Default to string
    return {
        'base_type': 'string',
        'specific_type': 'string',
        'sample_format': None,
        'unique_ratio': unique_ratio
    }

def validate_data(df, rules: list):
    """Validate data against a set of rules.
    Works with both Pandas and Polars DataFrames.
    
    Args:
        df: DataFrame to validate (Pandas or Polars)
        rules: List of validation rules
    """
    # Replace spaces in column names with underscores
    # Create a mapping of original column names to new column names
    column_mapping = {}
    for col in df.columns:
        if ' ' in col:
            new_col = col.replace(' ', '_')
            column_mapping[col] = new_col
    
    # Rename columns in the DataFrame
    if column_mapping:
        logger.info(f"Renaming columns with spaces: {column_mapping}")
        df = df.rename(columns=column_mapping)
        
        # Update column names in rules
        for rule in rules:
            kwargs = rule["kwargs"]
            # Update column_A if it exists and has spaces
            if "column_A" in kwargs and kwargs["column_A"] in column_mapping:
                kwargs["column_A"] = column_mapping[kwargs["column_A"]]
            # Update column_B if it exists and has spaces
            if "column_B" in kwargs and isinstance(kwargs["column_B"], str):
                if kwargs["column_B"] in column_mapping:
                    # Direct column reference
                    kwargs["column_B"] = column_mapping[kwargs["column_B"]]
                elif re.search(r"[\*\+\-/]", kwargs["column_B"]) and not kwargs["column_B"].isidentifier():
                    # Expression - replace all column names in the expression
                    expr = kwargs["column_B"]
                    for old_col, new_col in column_mapping.items():
                        # Use regex to match whole words only
                        expr = re.sub(r'(?<!\w)' + re.escape(old_col) + r'(?!\w)', new_col, expr)
                    kwargs["column_B"] = expr
            
            # Update columns in column_list if it exists
            if "column_list" in kwargs and isinstance(kwargs["column_list"], list):
                kwargs["column_list"] = [column_mapping.get(col, col) for col in kwargs["column_list"]]
                
            # Update expressions in meta data if they exist
            if "meta" in rule and isinstance(rule["meta"], dict):
                meta = rule["meta"]
                if "expression_column" in meta and isinstance(meta["expression_column"], str):
                    expr = meta["expression_column"]
                    for old_col, new_col in column_mapping.items():
                        # Use regex to match whole words only
                        expr = re.sub(r'(?<!\w)' + re.escape(old_col) + r'(?!\w)', new_col, expr)
                    meta["expression_column"] = expr
    
    # Apply calculations for any column expressions
    for rule in rules:
        exp_type = rule["expectation_type"]
        kwargs = rule["kwargs"]
        meta = rule.get("meta", {})
        
        # Get format settings from meta
        decimal_places = meta.get("decimal_places")
        date_format = meta.get("date_format")
        
        # Handle different types of expressions
        if exp_type == "expect_column_pair_values_to_be_equal":
            # Handle column_A - always a simple column name that needs conversion
            if "column_A" in kwargs:
                col_a = kwargs["column_A"]
                if isinstance(col_a, str):
                    col_type = detect_column_type(df, col_a)
                    if col_type == 'datetime':
                        df[f"__datetime_{col_a}"] = convert_to_datetime(df, col_a, date_format)
                        kwargs["column_A"] = f"__datetime_{col_a}"
                    else:
                        df[f"__decimal_{col_a}"] = convert_to_decimal(df, col_a, decimal_places)
                        kwargs["column_A"] = f"__decimal_{col_a}"
            
            # Case 1: column_B contains expression
            if "column_B" in kwargs and isinstance(kwargs["column_B"], str):
                col_b = kwargs["column_B"]
                if re.search(r"[\*\+\-/]", col_b) and not col_b.isidentifier():
                    new_col = f"__expr_col_{rule.get('rule_id', 'expr')}"
                    logger.info(f"🧮 Computing: {new_col} = {col_b}")
                    # Handle column names with spaces, detect types, and evaluate
                    modified_expr, _ = preprocess_expression(df, col_b, decimal_places, date_format, column_mapping)
                    df[new_col] = evaluate_expression(df, modified_expr, decimal_places)
                else:
                    # If column_B is a simple column name, detect its type and convert
                    col_info = detect_column_type(df, col_b)
                    # No need to replace spaces since we've already renamed all columns
                    if col_info['base_type'] == 'datetime':
                        df[f"__datetime_{col_b}"] = convert_to_datetime(df, col_b, date_format)
                        kwargs["column_B"] = f"__datetime_{col_b}"
                    else:
                        df[f"__decimal_{col_b}"] = convert_to_decimal(df, col_b, decimal_places)
                        kwargs["column_B"] = f"__decimal_{col_b}"
            
            # Case 2: value contains $eval
            elif "value" in kwargs and isinstance(kwargs["value"], dict) and "$eval" in kwargs["value"]:
                expression = kwargs["value"]["$eval"]
                new_col = f"__expr_col_{rule.get('rule_id', 'expr')}"
                logger.info(f"🧮 Computing: {new_col} = {expression}")
                df[new_col] = evaluate_expression(df, expression, decimal_places)
                # Update kwargs to use the new column
                kwargs["column_B"] = new_col

    # Create expectation suite
    suite = create_expectation_suite(rules)
    
    # Wrap in GE DataFrame
    ge_df = ge.from_pandas(df)

    # Run validation
    result = ge_df.validate(expectation_suite=suite)

    # Return result
    return {
        "message": "✅ Validation complete.",
        "validation_result": result.to_json_dict()
    }