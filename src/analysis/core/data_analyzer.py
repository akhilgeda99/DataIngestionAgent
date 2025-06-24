"""Core data analysis functionality."""

import pandas as pd
import polars as pl
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from logging import getLogger

from ..utils.type_converters import convert_polars_types
from ..utils.data_transformers import flatten_json, to_polars, optimize_dtypes
from ..processors.stats_processor import compute_stats, detect_quality_issues
from ...validation import validate_data

logger = getLogger(__name__)

class DataAnalyzer:
    def __init__(self, sample_size: int = 100000, chunk_size: int = 50000):
        """Initialize DataAnalyzer with configurable sampling and chunking.
        
        Args:
            sample_size: Number of rows to sample for initial analysis (default: 100k)
            chunk_size: Size of chunks for processing large datasets (default: 50k)
        """
        self.logger = getLogger(__name__)
        self.sample_size = sample_size
        self.chunk_size = chunk_size

    def _process_chunk(self, data: pd.DataFrame, start_idx: int, end_idx: int) -> Dict:
        """Process a single chunk of data."""
        try:
            # Get chunk of data
            chunk = data.iloc[start_idx:end_idx].copy()
            
            # Convert to Polars for efficient processing
            pl_chunk = to_polars(chunk)
            
            # Analyze chunk
            result = self._analyze_chunk(pl_chunk, data.dtypes.to_dict())
            
            # Clean up memory
            del chunk
            del pl_chunk
            gc.collect()
            
            return result
        except Exception as e:
            logger.error(f"Error processing chunk: {e}")
            return None

    def _analyze_chunk(self, pl_data: pl.DataFrame, original_dtypes: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze a chunk of data."""
        metrics = {
            "schema_info": {
                "numeric_columns": [],
                "string_columns": [],
                "date_columns": [],
                "datetime_columns": [],
                "nested_columns": [],
                "array_columns": [],
                "flattened_columns": []
            },
            "column_stats": {},
            "data_quality_issues": []
        }
        
        # Identify column types
        for col in pl_data.columns:
            col_series = pl_data[col]
            # Check Polars dtype first
            if col_series.dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
                metrics["schema_info"]["numeric_columns"].append(col)
            elif col_series.dtype == pl.Datetime:
                # Check if any values have time components
                sample_vals = col_series.drop_nulls().head(10).dt.strftime("%H:%M:%S").to_list()
                has_time = any(t != "00:00:00" for t in sample_vals)
                if has_time:
                    metrics["schema_info"]["datetime_columns"].append(col)
                else:
                    metrics["schema_info"]["date_columns"].append(col)
            elif col_series.dtype == pl.Date:
                metrics["schema_info"]["date_columns"].append(col)
            else:
                # Check original SQL type for numeric columns
                sql_type = str(original_dtypes.get(col, '')).lower()
                if any(t in sql_type for t in ['int', 'float', 'decimal', 'numeric']):
                    metrics["schema_info"]["numeric_columns"].append(col)
                else:
                    metrics["schema_info"]["string_columns"].append(col)
            
            # Compute optimized statistics
            metrics["column_stats"][col] = compute_stats(pl_data[col])
        
        return metrics

    def _combine_chunk_metrics(self, chunk_metrics: List[Dict]) -> Dict[str, Any]:
        """Combine metrics from multiple chunks."""
        if not chunk_metrics:
            return {}
        
        # Start with a clean combined metrics structure
        combined = {
            "total_rows": sum(m.get("total_rows", 0) for m in chunk_metrics),
            "total_columns": chunk_metrics[0].get("total_columns", 0),
            "column_stats": {},
            "data_quality_issues": [],
            "analysis_info": chunk_metrics[0].get("analysis_info", {})
        }
        
        # Copy any metadata from the first chunk
        if "metadata" in chunk_metrics[0]:
            combined["metadata"] = chunk_metrics[0]["metadata"]
            
        # Copy any additional fields we want to preserve
        for key in ["schema", "table_name", "database", "columns", "schema_info"]:
            if key in chunk_metrics[0]:
                combined[key] = chunk_metrics[0][key]
        
        # If schema_info wasn't in the first chunk, initialize it
        if "schema_info" not in combined:
            schema_categories = ["numeric_columns", "string_columns", "date_columns", 
                               "nested_columns", "array_columns", "flattened_columns", "datetime_columns"]
            combined["schema_info"] = {category: [] for category in schema_categories}
            
            # In this case, combine schema info from all chunks
            for chunk in chunk_metrics:
                for category in schema_categories:
                    combined["schema_info"][category] = list(set(
                        combined["schema_info"][category] + 
                        chunk.get("schema_info", {}).get(category, [])
                    ))
        
        # Get all unique column names from all chunks
        all_columns = set()
        for chunk in chunk_metrics:
            all_columns.update(chunk.get("column_stats", {}).keys())
        
        # Process each column
        for col in all_columns:
            # Get all non-null stats for this column from chunks
            col_chunks = [
                chunk["column_stats"][col] for chunk in chunk_metrics 
                if col in chunk.get("column_stats", {})
            ]
            
            if col_chunks:
                # Use the first chunk's stats as base
                combined["column_stats"][col] = col_chunks[0]
                
                # Update counts by summing across chunks
                combined["column_stats"][col].update({
                    "total_rows": sum(c.get("total_rows", 0) for c in col_chunks),
                    "null_count": sum(c.get("null_count", 0) for c in col_chunks),
                    "unique_count": sum(c.get("unique_count", 0) for c in col_chunks)
                })
                
                # Recalculate percentages
                total = combined["column_stats"][col]["total_rows"]
                null_count = combined["column_stats"][col]["null_count"]
                unique_count = combined["column_stats"][col]["unique_count"]
                
                if total > 0:
                    combined["column_stats"][col]["null_percentage"] = round((null_count / total * 100), 2)
                    if total - null_count > 0:
                        combined["column_stats"][col]["unique_percentage"] = round((unique_count / (total - null_count) * 100), 2)
        
        return combined

    def analyze_data_quality(self, data: pd.DataFrame, use_sampling: bool = True) -> Dict[str, Any]:
        """Analyze data quality and return comprehensive metrics.
        
        Args:
            data: pandas DataFrame to analyze
            use_sampling: Whether to use sampling for large datasets
            validate_data: Whether to perform data validation using JSON schema
        """
        try:
            # Record total rows before any processing
            total_rows = len(data)
            
            # Flatten any nested JSON structures
            data = flatten_json(data)
            
            # Optimize memory usage by converting to appropriate dtypes
            data = optimize_dtypes(data)
            
            # Process large datasets in chunks without sampling
            chunk_size = min(100000, max(10000, total_rows // 10))  # Dynamic chunk size
            if total_rows > chunk_size:
                logger.info(f"Processing {total_rows} records in chunks of {chunk_size}")
                chunk_metrics = []
                processed_rows = 0
                
                # Create chunks for parallel processing
                chunks = []
                for start_idx in range(0, total_rows, chunk_size):
                    end_idx = min(start_idx + chunk_size, total_rows)
                    chunks.append((start_idx, end_idx))
                
                # Process chunks in parallel
                with ThreadPoolExecutor(max_workers=min(8, len(chunks))) as executor:
                    future_to_chunk = {
                        executor.submit(self._process_chunk, data, start_idx, end_idx): (start_idx, end_idx)
                        for start_idx, end_idx in chunks
                    }
                    
                    for future in as_completed(future_to_chunk):
                        chunk_result = future.result()
                        if chunk_result:
                            chunk_metrics.append(chunk_result)
                            start_idx, end_idx = future_to_chunk[future]
                            processed_rows += end_idx - start_idx
                            
                            logger.info(f"Processed {processed_rows}/{total_rows} rows")
                
                # Combine metrics from all chunks
                metrics = self._combine_chunk_metrics(chunk_metrics)
                
                # Add analysis info
                metrics["analysis_info"] = {
                    "total_rows": total_rows,
                    "processed_rows": processed_rows,
                    "processed_in_chunks": True,
                    "parallel_processing": True
                }
                
                # Calculate data quality issues for chunked data
                detect_quality_issues(metrics)
                
            else:
                # For small datasets, process everything at once
                try:
                    pl_data = to_polars(data)
                    metrics = self._analyze_chunk(pl_data, data.dtypes.to_dict())
                    
                    metrics["analysis_info"] = {
                        "total_rows": total_rows,
                        "processed_rows": total_rows,
                        "processed_in_chunks": False,
                        "parallel_processing": False
                    }
                    
                    # Calculate data quality issues for non-chunked data
                    detect_quality_issues(metrics)
                    
                except Exception as e:
                    logger.error(f"Error analyzing data: {e}")
                    return {
                        "error": str(e),
                        "analysis_info": {
                            "total_rows": total_rows,
                            "processed_rows": 0,
                            "error_occurred": True
                        }
                    }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error analyzing data quality: {e}")
            return {
                "error": str(e),
                "analysis_info": {
                    "total_rows": total_rows if 'total_rows' in locals() else 0,
                    "processed_rows": 0,
                    "error_occurred": True
                }
            }
