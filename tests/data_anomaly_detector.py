"""Module for detecting anomalies between datasets."""

import pandas as pd
import numpy as np
from logging import getLogger
from typing import Dict, List, Tuple, Any
from datetime import datetime

logger = getLogger(__name__)

class DataAnomalyDetector:
    def __init__(self):
        """Initialize the anomaly detector."""
        self.threshold = 0.1  # Default threshold for numerical comparisons (10% difference)
        
    def compare_datasets(self, 
                        previous_df: pd.DataFrame, 
                        current_df: pd.DataFrame,
                        date_column: str = None) -> Dict[str, Any]:
        """
        Compare two datasets and detect anomalies between them.
        
        Args:
            previous_df: DataFrame containing yesterday's data
            current_df: DataFrame containing today's data
            date_column: Optional name of the date column for time-based analysis
            
        Returns:
            Dict containing anomaly report
        """
        try:
            anomalies = {
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'previous_rows': len(previous_df),
                    'current_rows': len(current_df),
                    'row_difference': len(current_df) - len(previous_df),
                    'column_differences': []
                },
                'column_anomalies': {},
                'new_values': {},
                'missing_values': {}
            }
            
            # Compare common columns
            common_columns = set(previous_df.columns) & set(current_df.columns)
            
            for col in common_columns:
                col_anomalies = self._analyze_column_changes(
                    previous_df[col],
                    current_df[col],
                    date_column
                )
                
                if col_anomalies:
                    anomalies['column_anomalies'][col] = col_anomalies
            
            # Check for new or missing columns
            new_cols = set(current_df.columns) - set(previous_df.columns)
            missing_cols = set(previous_df.columns) - set(current_df.columns)
            
            if new_cols:
                anomalies['summary']['column_differences'].append(
                    f"New columns found: {', '.join(new_cols)}"
                )
            
            if missing_cols:
                anomalies['summary']['column_differences'].append(
                    f"Missing columns: {', '.join(missing_cols)}"
                )
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Error comparing datasets: {e}")
            raise
    
    def _analyze_column_changes(self,
                              prev_series: pd.Series,
                              curr_series: pd.Series,
                              date_column: str = None) -> Dict[str, Any]:
        """Analyze changes in a specific column between two datasets."""
        
        anomalies = {}
        
        # Handle numeric columns
        if pd.api.types.is_numeric_dtype(prev_series) and pd.api.types.is_numeric_dtype(curr_series):
            prev_stats = prev_series.describe()
            curr_stats = curr_series.describe()
            
            # Compare basic statistics
            for stat in ['mean', 'std', 'min', 'max']:
                prev_val = prev_stats[stat]
                curr_val = curr_stats[stat]
                
                if prev_val != 0:  # Avoid division by zero
                    pct_change = abs((curr_val - prev_val) / prev_val)
                    if pct_change > self.threshold:
                        anomalies[f'{stat}_change'] = {
                            'previous': float(prev_val),
                            'current': float(curr_val),
                            'percentage_change': float(pct_change * 100)
                        }
        
        # Handle categorical columns
        else:
            prev_value_counts = prev_series.value_counts()
            curr_value_counts = curr_series.value_counts()
            
            # Find new and missing categories
            new_categories = set(curr_value_counts.index) - set(prev_value_counts.index)
            missing_categories = set(prev_value_counts.index) - set(curr_value_counts.index)
            
            if new_categories:
                anomalies['new_categories'] = list(new_categories)
            if missing_categories:
                anomalies['missing_categories'] = list(missing_categories)
            
            # Compare distribution changes for common categories
            common_categories = set(prev_value_counts.index) & set(curr_value_counts.index)
            distribution_changes = []
            
            for category in common_categories:
                prev_pct = prev_value_counts[category] / len(prev_series)
                curr_pct = curr_value_counts[category] / len(curr_series)
                
                if abs(curr_pct - prev_pct) > self.threshold:
                    distribution_changes.append({
                        'category': str(category),
                        'previous_percentage': float(prev_pct * 100),
                        'current_percentage': float(curr_pct * 100),
                        'difference': float((curr_pct - prev_pct) * 100)
                    })
            
            if distribution_changes:
                anomalies['distribution_changes'] = distribution_changes
        
        return anomalies if anomalies else None
    
    def set_threshold(self, threshold: float):
        """Set the threshold for detecting numerical anomalies."""
        if 0 < threshold < 1:
            self.threshold = threshold
        else:
            raise ValueError("Threshold must be between 0 and 1")
