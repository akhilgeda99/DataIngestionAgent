"""Statistical processing utilities for data analysis."""

import pandas as pd
import polars as pl
import re
from typing import Dict, Any
from logging import getLogger

logger = getLogger(__name__)

def compute_stats(series: pl.Series) -> Dict[str, Any]:
    """Compute statistics for a single column."""
    try:
        # Get sample values first
        sample_values = series.drop_nulls().unique().head(10).to_list()
        
        # Format sample values
        formatted_samples = [
            value.strftime("%Y-%m-%d") if str(series.dtype).lower() in ['date', 'datetime'] and value is not None
            else str(value) if value is not None else 'null'
            for value in sample_values
        ]

        # Get value counts
        value_counts = {}
        try:
            if str(series.dtype).lower() in ['date', 'datetime']:
                # Format dates to strings first
                formatted_series = series.map_elements(
                    lambda x: x.strftime("%Y-%m-%d") if x is not None else None,
                    return_dtype=pl.String
                )
                pd_series = formatted_series.to_pandas()
            else:
                pd_series = series.to_pandas()
            
            counts = pd_series.value_counts()
            for value, count in zip(counts.index, counts.values):
                value_str = str(value) if value is not None else 'null'
                value_counts[value_str] = {
                    "count": int(count),
                    "percentage": round((int(count) / len(series) * 100), 2)
                }
        except Exception as e:
            logger.warning(f"Error computing value counts: {e}")

        stats = {
            "column_name": series.name,
            "data_type": str(series.dtype),
            "total_rows": len(series),
            "null_count": series.null_count(),
            "null_percentage": round((series.null_count() / len(series) * 100), 2),
            "unique_count": series.n_unique(),
            "unique_percentage": round((series.n_unique() / (len(series) - series.null_count()) * 100), 2),
            "sample_values": formatted_samples,
            "value_counts": value_counts
        }
        
        return stats
    except Exception as e:
        logger.error(f"Error computing stats: {e}")
        return {"error": str(e)}

def detect_quality_issues(metrics: Dict[str, Any]) -> None:
    """Detect data quality issues in the combined metrics."""
    # Track which columns we've already processed for each issue type
    processed_issues = set()
    total_rows = metrics.get("total_rows", 0)
    
    # Check if we have any rows to analyze
    if total_rows == 0:
        metrics["data_quality_issues"] = []
        return
    
    # Check if column_stats is a dictionary before accessing keys
    column_stats = metrics.get("column_stats", {})
    if not isinstance(column_stats, dict):
        logger.error(f"Expected column_stats to be a dictionary, got {type(column_stats)}")
        metrics["data_quality_issues"] = []
        return
        
    all_columns = column_stats.keys()
    schema_info = metrics.get("schema_info", {})
    
    # Initialize data_quality_issues if not present
    if "data_quality_issues" not in metrics:
        metrics["data_quality_issues"] = []
    # Else clear existing issues to avoid duplicates
    else:
        metrics["data_quality_issues"] = []
    
    for col in all_columns:
        # Initialize variables for each column to avoid reference errors
        samples = []
        str_samples = []
        
        col_stats = column_stats.get(col, {})
        null_count = col_stats.get("null_count", 0)
        
        # Get sample values for this column once and reuse throughout
        samples = col_stats.get("sample_values", [])
        str_samples = [str(sample) for sample in samples if sample is not None]
        
        # Check for high null percentage
        if null_count > 0 and total_rows > 0:
            null_percentage = (null_count / total_rows) * 100
            if null_percentage > 20:  # More than 20% nulls
                issue_key = f"high_null_{col}"
                if issue_key not in processed_issues:
                    metrics["data_quality_issues"].append({
                        "column": col,
                        "issue_type": "high_null_percentage",
                        "description": f"Column has {null_percentage:.1f}% null values",
                        "severity": "high" if null_percentage > 50 else "medium"
                    })
                    processed_issues.add(issue_key)
        
        # Check for inconsistent data types in string columns
        if col in schema_info.get("string_columns", []):
            # Check for mixed numeric and non-numeric values
            numeric_pattern = re.compile(r'^-?\d*\.?\d+$')
            mixed_types = False
            numeric_count = 0
            
            for sample in str_samples:
                if numeric_pattern.match(str(sample)):
                    numeric_count += 1
                
            if len(str_samples) > 0 and 0 < numeric_count < len(str_samples):  # Some but not all are numeric
                issue_key = f"mixed_types_{col}"
                if issue_key not in processed_issues:
                    metrics["data_quality_issues"].append({
                        "column": col,
                        "issue_type": "mixed_data_types",
                        "description": "Column contains mix of numeric and non-numeric values",
                        "severity": "medium"
                    })
                    processed_issues.add(issue_key)
            
            # Check for inconsistent casing
            if any(s.isupper() for s in str_samples) and any(s.islower() for s in str_samples):
                issue_key = f"inconsistent_case_{col}"
                if issue_key not in processed_issues:
                    metrics["data_quality_issues"].append({
                        "column": col,
                        "issue_type": "inconsistent_casing",
                        "description": "Column contains mix of upper and lower case values",
                        "severity": "low"
                    })
                    processed_issues.add(issue_key)
            
            # Check for special characters
            special_char_pattern = re.compile(r'[^a-zA-Z0-9\s]')
            if any(special_char_pattern.search(s) for s in str_samples):
                issue_key = f"special_chars_{col}"
                if issue_key not in processed_issues:
                    metrics["data_quality_issues"].append({
                        "column": col,
                        "issue_type": "special_characters",
                        "description": "Column contains special characters",
                        "severity": "low"
                    })
                    processed_issues.add(issue_key)
        
        # Check for high cardinality in non-numeric columns
        if col not in schema_info.get("numeric_columns", []):
            unique_count = col_stats.get("unique_count", 0)
            non_null_count = total_rows - null_count
            if unique_count > 0 and non_null_count > 0:
                unique_percentage = (unique_count / non_null_count) * 100
                if unique_percentage > 90:  # More than 90% unique values
                    issue_key = f"high_cardinality_{col}"
                    if issue_key not in processed_issues:
                        metrics["data_quality_issues"].append({
                            "column": col,
                            "issue_type": "high_cardinality",
                            "description": f"Column has {unique_percentage:.1f}% unique values",
                            "severity": "medium"
                        })
                        processed_issues.add(issue_key)
