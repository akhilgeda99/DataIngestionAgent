"""Data analysis module for extracting insights and metrics from data.

This module provides comprehensive data analysis capabilities including:
- Data quality assessment
- Anomaly detection
- Statistical analysis
- Pattern recognition and insights generation
- Correlation analysis
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any

class DataAnalyzer:
    """Handles data analysis tasks including quality metrics and anomaly detection.
    
    This class provides a suite of analysis methods to extract insights and
    assess data quality. Key features include:
    - Comprehensive data quality metrics
    - Statistical analysis of numerical and categorical data
    - Anomaly detection using statistical methods
    - Pattern recognition and correlation analysis
    """
    
    def analyze_data_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data quality and return comprehensive metrics.
        
        Performs a thorough analysis of data quality including:
        - Basic statistics (row/column counts)
        - Missing value analysis
        - Data type information
        - Unique value counts
        - Sample values
        - Numerical statistics (mean, std, quartiles)
        - Correlation analysis
        
        Args:
            data (pd.DataFrame): Input data to analyze.
            
        Returns:
            Dict[str, Any]: Comprehensive quality metrics and statistics.
        """
        metrics = {
            "total_rows": len(data),
            "total_columns": len(data.columns),
            "missing_values_per_column": data.isnull().sum().to_dict(),
            "data_types": {col: str(dtype) for col, dtype in data.dtypes.items()},
            "unique_values_per_column": {col: int(data[col].nunique()) for col in data.columns},
            "sample_values": {col: data[col].head().tolist() for col in data.columns}
        }
        
        # Add basic statistics for numerical columns
        numerical_cols = data.select_dtypes(include=[np.number]).columns
        if not numerical_cols.empty:
            metrics["numerical_statistics"] = {
                col: {
                    "mean": float(data[col].mean()),
                    "std": float(data[col].std()),
                    "min": float(data[col].min()),
                    "max": float(data[col].max()),
                    "quartiles": {
                        "25": float(data[col].quantile(0.25)),
                        "50": float(data[col].quantile(0.50)),
                        "75": float(data[col].quantile(0.75))
                    }
                } for col in numerical_cols
            }
            
            # Add correlation matrix for numerical columns
            if len(numerical_cols) > 1:
                metrics["correlations"] = data[numerical_cols].corr().to_dict()
        
        return metrics
    
    def detect_anomalies(self, data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Detect anomalies in specified columns using statistical methods.
        
        Uses z-score based approach to identify anomalies:
        - Values beyond 3 standard deviations are marked as anomalies
        - Values beyond 5 standard deviations are marked as extreme anomalies
        - Anomaly scores are calculated based on deviation from mean
        
        Args:
            data (pd.DataFrame): Input data to analyze.
            columns (List[str]): List of columns to check for anomalies.
            
        Returns:
            pd.DataFrame: Original data with additional columns for anomaly
                        indicators and scores.
        """
        for col in columns:
            if pd.api.types.is_numeric_dtype(data[col]):
                mean = data[col].mean()
                std = data[col].std()
                # Mark values beyond 3 standard deviations as anomalies
                data[f"{col}_is_anomaly"] = abs(data[col] - mean) > 3 * std
                # Calculate anomaly score (0 to 1, higher means more anomalous)
                data[f"{col}_anomaly_score"] = abs(data[col] - mean) / (3 * std)
                
                # Add additional anomaly statistics
                data[f"{col}_zscore"] = (data[col] - mean) / std
                data[f"{col}_is_extreme"] = abs(data[f"{col}_zscore"]) > 5  # More extreme anomalies
        
        return data
    
    def get_column_insights(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Generate insights about column relationships and patterns.
        
        Performs advanced analysis to discover:
        - Highly correlated numerical columns (correlation > 0.7)
        - Columns with unusually high variance
        - Categorical column statistics and entropy
        - Distribution patterns and outliers
        
        Args:
            data (pd.DataFrame): Input data to analyze.
            
        Returns:
            Dict[str, Any]: Dictionary containing various insights and
                          patterns discovered in the data.
        """
        insights = {}
        
        # Analyze numerical columns
        numerical_cols = data.select_dtypes(include=[np.number]).columns
        if not numerical_cols.empty:
            # Find highly correlated columns
            if len(numerical_cols) > 1:
                corr_matrix = data[numerical_cols].corr()
                high_correlations = []
                for i in range(len(numerical_cols)):
                    for j in range(i + 1, len(numerical_cols)):
                        correlation = corr_matrix.iloc[i, j]
                        if abs(correlation) > 0.7:  # Strong correlation threshold
                            high_correlations.append({
                                "column1": numerical_cols[i],
                                "column2": numerical_cols[j],
                                "correlation": float(correlation)
                            })
                insights["high_correlations"] = high_correlations
            
            # Identify columns with high variance
            variances = data[numerical_cols].var()
            high_variance_cols = variances[variances > variances.mean() + variances.std()].index.tolist()
            insights["high_variance_columns"] = high_variance_cols
        
        # Analyze categorical columns
        categorical_cols = data.select_dtypes(include=['object']).columns
        if not categorical_cols.empty:
            category_stats = {}
            for col in categorical_cols:
                value_counts = data[col].value_counts()
                category_stats[col] = {
                    "unique_values": len(value_counts),
                    "most_common": value_counts.head(3).to_dict(),
                    "distribution_entropy": float(-(value_counts / len(data) * np.log2(value_counts / len(data))).sum())
                }
            insights["categorical_statistics"] = category_stats
        
        return insights
