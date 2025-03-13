"""AI Agent module for orchestrating data processing and analysis.

This module serves as the main coordinator for AI-powered data operations,
integrating preprocessing and analysis components to provide comprehensive
data insights and quality metrics.
"""

from .preprocessing.data_preprocessor import DataPreprocessor
from .analysis.data_analyzer import DataAnalyzer
import pandas as pd

class AIDataAgent:
    """Main AI agent that orchestrates data preprocessing and analysis.
    
    This class serves as the central coordinator for all AI-powered data operations,
    managing the interaction between data preprocessing and analysis components.
    It provides a high-level interface for data processing, quality analysis,
    anomaly detection, and insight generation.
    """
    
    def __init__(self):
        """Initialize the AI agent with preprocessor and analyzer components."""
        self.preprocessor = DataPreprocessor()
        self.analyzer = DataAnalyzer()
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process data using AI techniques.
        
        Applies a series of AI-powered preprocessing steps to clean and
        standardize the input data.
        
        Args:
            data (pd.DataFrame): Raw input data to process.
            
        Returns:
            pd.DataFrame: Processed and cleaned data.
        """
        return self.preprocessor.process_data(data)
    
    def analyze_data_quality(self, data: pd.DataFrame) -> dict:
        """Analyze data quality and return metrics.
        
        Performs comprehensive quality analysis including missing values,
        data types, and statistical distributions.
        
        Args:
            data (pd.DataFrame): Data to analyze.
            
        Returns:
            dict: Dictionary containing various quality metrics and statistics.
        """
        return self.analyzer.analyze_data_quality(data)
    
    def detect_anomalies(self, data: pd.DataFrame, columns: list) -> pd.DataFrame:
        """Detect anomalies in specified columns.
        
        Uses statistical and AI techniques to identify unusual or outlier
        values in the specified columns.
        
        Args:
            data (pd.DataFrame): Data to analyze for anomalies.
            columns (list): List of column names to check for anomalies.
            
        Returns:
            pd.DataFrame: Original data with additional columns for anomaly scores
                        and indicators.
        """
        return self.analyzer.detect_anomalies(data, columns)
    
    def get_data_insights(self, data: pd.DataFrame) -> dict:
        """Get comprehensive insights about the data.
        
        Generates detailed insights about data patterns, relationships,
        and potential issues.
        
        Args:
            data (pd.DataFrame): Data to analyze.
            
        Returns:
            dict: Dictionary containing various insights and patterns found
                 in the data.
        """
        return self.analyzer.get_column_insights(data)
