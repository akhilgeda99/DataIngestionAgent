"""Data transformation utilities for analysis."""

import pandas as pd
import polars as pl
import numpy as np
from datetime import time
from typing import Dict
from logging import getLogger

logger = getLogger(__name__)

def flatten_json(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten nested JSON structures in DataFrame while preserving other data types."""
    try:
        # Copy to avoid modifying original
        df = df.copy()
        
        # Find columns containing JSON/dict structures
        for col in df.columns:
            if df[col].dtype == 'object':
                # Get non-null values for checking
                non_null_values = df[col].dropna()
                if non_null_values.empty:
                    continue
                
                # Check if column contains ANY JSON/dict by sampling
                sample_size = min(10, len(non_null_values))
                samples = non_null_values.head(sample_size)
                
                # Check if ANY sampled values are dict or list
                has_nested = any(isinstance(x, (dict, list)) for x in samples)
                if not has_nested:
                    continue
                
                # Create a new column for non-nested values
                df[f"{col}_value"] = df[col].apply(
                    lambda x: x if not isinstance(x, (dict, list)) and pd.notna(x) else pd.NA
                )
                
                # Process nested structures
                nested_values = df[df[col].apply(lambda x: isinstance(x, (dict, list)) if pd.notna(x) else False)]
                if not nested_values.empty:
                    sample = next((x for x in samples if isinstance(x, (dict, list))), None)
                    if isinstance(sample, dict):
                        # For dictionary, create new columns
                        for key in sample.keys():
                            new_col = f"{col}_{key}"
                            df[new_col] = df[col].apply(
                                lambda x: x.get(key) if isinstance(x, dict) else pd.NA
                            )
                    elif isinstance(sample, list) and sample:
                        # For lists, create columns for common keys if elements are dicts
                        if isinstance(sample[0], dict):
                            # Get all unique keys from all dictionaries in lists
                            keys = set()
                            df[col].dropna().apply(
                                lambda x: [keys.update(item.keys()) 
                                         for item in x if isinstance(item, dict)]
                                if isinstance(x, list) else None
                            )
                            
                            # Create columns for each key
                            for key in keys:
                                new_col = f"{col}_{key}"
                                df[new_col] = df[col].apply(
                                    lambda x: [item.get(key) for item in x if isinstance(item, dict)]
                                    if isinstance(x, list) else pd.NA
                                )
                        else:
                            # For simple lists, store the length and values
                            df[f"{col}_length"] = df[col].apply(
                                lambda x: len(x) if isinstance(x, list) and pd.notna(x) else pd.NA
                            )
                            df[f"{col}_items"] = df[col].apply(
                                lambda x: x if isinstance(x, list) and pd.notna(x) else pd.NA
                            )
                
                # Drop the original nested column only if we created new columns
                if len(df.columns) > len(df.columns.drop(col)):
                    df = df.drop(columns=[col])
        
        return df
    except Exception as e:
        logger.error(f"Error flattening JSON: {e}")
        return df

def to_polars(df: pd.DataFrame) -> pl.DataFrame:
    """Convert pandas DataFrame to Polars DataFrame with proper type handling."""
    try:
        # Detect numeric and datetime columns
        num_cols = df.select_dtypes(include=['int', 'float']).columns.tolist()
        date_cols = []
        datetime_cols = []
        
        # Identify date and datetime columns
        for col in df.select_dtypes(include=['datetime64', 'datetime64[ns]']).columns:
            if all(pd.notna(t) and t.time() == time(0, 0) for t in df[col].dropna()):
                date_cols.append(col)
            else:
                datetime_cols.append(col)
        
        # Create schema for Polars conversion
        schema = {}
        for col in df.columns:
            if col in num_cols:
                if df[col].isna().any():
                    schema[col] = pl.Float64
                else:
                    # Check if values are within int32 or int64 range
                    min_val = df[col].min()
                    max_val = df[col].max()
                    if min_val >= -2147483648 and max_val <= 2147483647:
                        schema[col] = pl.Int32
                    elif min_val >= -9223372036854775808 and max_val <= 9223372036854775807:
                        schema[col] = pl.Int64
                    else:
                        schema[col] = pl.Float64
            elif col in date_cols:
                # Keep dates as dates in Polars
                schema[col] = pl.Date
            elif col in datetime_cols:
                # Keep datetimes as datetimes in Polars
                schema[col] = pl.Datetime
            else:
                # Convert bytes to strings first if needed
                if df[col].dtype == 'object':
                    try:
                        # Check if column contains bytes
                        if df[col].notna().any() and isinstance(df[col].iloc[0], bytes):
                            df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
                    except Exception as e:
                        logger.warning(f"Error converting bytes to string in column {col}: {e}")
                schema[col] = pl.Utf8

        try:
            # For date/datetime columns, ensure they're in the correct format
            for col in date_cols:
                df[col] = pd.to_datetime(df[col]).dt.date
            for col in datetime_cols:
                df[col] = pd.to_datetime(df[col])
                
            # Convert DataFrame with explicit schema
            return pl.from_pandas(df, schema_overrides=schema)
        except Exception as e:
            logger.warning(f"Error in final Polars conversion: {e}")
            # Fallback: convert everything to strings
            try:
                df = df.astype(str)
                return pl.from_pandas(df)
            except Exception as e2:
                logger.error(f"Error in fallback conversion: {e2}")
                raise
        
    except Exception as e:
        logger.error(f"Error converting DataFrame to Polars: {e}")
        raise

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage by converting to appropriate dtypes."""
    try:
        # Process numeric columns
        numeric_cols = df.select_dtypes(include=['int', 'float']).columns
        for col in numeric_cols:
            # Skip if column is all null
            if df[col].isna().all():
                continue
                
            # Get min and max values
            col_min = df[col].min()
            col_max = df[col].max()
            
            # Convert to smallest possible integer type if no nulls
            if not df[col].isna().any() and col_min.is_integer() and col_max.is_integer():
                if col_min >= 0:
                    if col_max <= 255:
                        df[col] = df[col].astype(np.uint8)
                    elif col_max <= 65535:
                        df[col] = df[col].astype(np.uint16)
                    elif col_max <= 4294967295:
                        df[col] = df[col].astype(np.uint32)
                    else:
                        df[col] = df[col].astype(np.uint64)
                else:
                    if col_min >= -128 and col_max <= 127:
                        df[col] = df[col].astype(np.int8)
                    elif col_min >= -32768 and col_max <= 32767:
                        df[col] = df[col].astype(np.int16)
                    elif col_min >= -2147483648 and col_max <= 2147483647:
                        df[col] = df[col].astype(np.int32)
                    else:
                        df[col] = df[col].astype(np.int64)
            # Convert to float32 if precision allows
            elif not df[col].isna().any():
                float32_series = df[col].astype(np.float32)
                if (df[col] - float32_series).abs().max() < 1e-6:
                    df[col] = float32_series
        
        # Process string columns
        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            # Skip if column is all null
            if df[col].isna().all():
                continue
                
            # Convert to category if beneficial
            nunique = df[col].nunique()
            if nunique / len(df) < 0.5:  # Convert if less than 50% unique values
                df[col] = df[col].astype('category')
        
        return df
    except Exception as e:
        logger.error(f"Error optimizing dtypes: {e}")
        return df
