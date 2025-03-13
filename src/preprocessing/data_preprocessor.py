"""Data preprocessing module for cleaning and standardizing input data.

This module provides comprehensive data preprocessing capabilities including:
- Numerical data normalization
- Missing value handling
- Data type standardization
- Automated data cleaning and preparation
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from typing import Dict, List, Optional

class DataPreprocessor:
    """Handles data preprocessing tasks like normalization and missing value handling.
    
    This class provides a suite of preprocessing methods to clean and standardize
    data before analysis. It handles common preprocessing tasks such as:
    - Numerical data normalization using StandardScaler
    - Intelligent missing value imputation
    - Data type standardization and conversion
    """
    
    def __init__(self):
        """Initialize the preprocessor with a StandardScaler for normalization."""
        self.scaler = StandardScaler()
        
    def normalize_numerical(self, data: pd.DataFrame) -> pd.DataFrame:
        """Normalize numerical columns in the dataset using StandardScaler.
        
        Applies standardization to numerical columns to ensure they have
        zero mean and unit variance, which is important for many machine
        learning algorithms.
        
        Args:
            data (pd.DataFrame): Input data to normalize.
            
        Returns:
            pd.DataFrame: DataFrame with normalized numerical columns.
        """
        numerical_cols = data.select_dtypes(include=[np.number]).columns
        if not numerical_cols.empty:
            data[numerical_cols] = self.scaler.fit_transform(data[numerical_cols])
        return data
    
    def handle_missing_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset using appropriate strategies.
        
        Applies intelligent missing value imputation:
        - Numerical columns: Filled with median values
        - Categorical columns: Filled with mode (most frequent value)
        
        Args:
            data (pd.DataFrame): Input data with missing values.
            
        Returns:
            pd.DataFrame: DataFrame with missing values handled.
        """
        numerical_columns = data.select_dtypes(include=[np.number]).columns
        categorical_columns = data.select_dtypes(include=['object']).columns
        
        # Fill numerical columns with median
        data[numerical_columns] = data[numerical_columns].fillna(data[numerical_columns].median())
        
        # Fill categorical columns with mode
        for col in categorical_columns:
            data[col] = data[col].fillna(data[col].mode().iloc[0] if not data[col].mode().empty else 'Unknown')
        
        return data
    
    def standardize_data_types(self, data: pd.DataFrame) -> pd.DataFrame:
        """Convert data types to standard Python types.
        
        Ensures consistent data types across columns:
        - Integer columns -> int64
        - Float columns -> float64
        - Boolean columns -> bool
        - Other columns -> str
        
        Args:
            data (pd.DataFrame): Input data to standardize.
            
        Returns:
            pd.DataFrame: DataFrame with standardized data types.
        """
        for col in data.columns:
            if pd.api.types.is_integer_dtype(data[col]):
                data[col] = data[col].astype('int64')
            elif pd.api.types.is_float_dtype(data[col]):
                data[col] = data[col].astype('float64')
            elif pd.api.types.is_bool_dtype(data[col]):
                data[col] = data[col].astype('bool')
            else:
                data[col] = data[col].astype('str')
        return data
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply all preprocessing steps to the data.
        
        Executes the complete preprocessing pipeline:
        1. Handle missing values
        2. Normalize numerical data
        3. Standardize data types
        
        Args:
            data (pd.DataFrame): Raw input data.
            
        Returns:
            pd.DataFrame: Fully preprocessed data ready for analysis.
        """
        data = self.handle_missing_values(data)
        data = self.normalize_numerical(data)
        data = self.standardize_data_types(data)
        return data
