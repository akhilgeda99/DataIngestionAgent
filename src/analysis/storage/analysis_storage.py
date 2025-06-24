"""Module for storing analysis results in the database."""

import pandas as pd
from datetime import datetime
from logging import getLogger
from typing import Dict, Any, Optional
import json

from ...database import Database

logger = getLogger(__name__)

class AnalysisStorage:
    def __init__(self, db: Database):
        """Initialize the analysis storage with database connection."""
        self.db = db
    
    def store_profile_results(self, 
                            profile_data: Dict[str, Any],
                            dataset_name: str,
                            timestamp: Optional[datetime] = None) -> int:
        """
        Store data profiling results in the database.
        
        Args:
            profile_data: Dictionary containing profile results
            dataset_name: Name of the analyzed dataset
            timestamp: Optional timestamp for the analysis
            
        Returns:
            ID of the stored profile record
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        # Create profiles table if it doesn't exist
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS data_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_name VARCHAR(255),
                timestamp DATETIME,
                summary JSON,
                column_profiles JSON,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert profile data
        query = """
            INSERT INTO data_profiles (dataset_name, timestamp, summary, column_profiles)
            VALUES (?, ?, ?, ?)
        """
        
        summary = profile_data.get('summary', {})
        columns = profile_data.get('columns', {})
        
        params = (
            dataset_name,
            timestamp,
            json.dumps(summary),
            json.dumps(columns)
        )
        
        return self.db.execute(query, params)
    
    def store_anomaly_results(self,
                            anomaly_data: Dict[str, Any],
                            dataset_name: str,
                            comparison_date: datetime) -> int:
        """
        Store anomaly detection results in the database.
        
        Args:
            anomaly_data: Dictionary containing anomaly detection results
            dataset_name: Name of the analyzed dataset
            comparison_date: Date of the comparison
            
        Returns:
            ID of the stored anomaly record
        """
        # Create anomalies table if it doesn't exist
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS data_anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_name VARCHAR(255),
                comparison_date DATE,
                summary JSON,
                column_anomalies JSON,
                new_values JSON,
                missing_values JSON,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert anomaly data
        query = """
            INSERT INTO data_anomalies (
                dataset_name, 
                comparison_date,
                summary,
                column_anomalies,
                new_values,
                missing_values
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        params = (
            dataset_name,
            comparison_date,
            json.dumps(anomaly_data.get('summary', {})),
            json.dumps(anomaly_data.get('column_anomalies', {})),
            json.dumps(anomaly_data.get('new_values', {})),
            json.dumps(anomaly_data.get('missing_values', {}))
        )
        
        return self.db.execute(query, params)
    
    def get_latest_profile(self, dataset_name: str) -> Dict[str, Any]:
        """Retrieve the latest profile for a dataset."""
        query = """
            SELECT * FROM data_profiles
            WHERE dataset_name = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """
        
        result = self.db.fetch_one(query, (dataset_name,))
        if result:
            return {
                'id': result[0],
                'dataset_name': result[1],
                'timestamp': result[2],
                'summary': json.loads(result[3]),
                'column_profiles': json.loads(result[4]),
                'created_at': result[5]
            }
        return None
    
    def get_latest_anomalies(self, dataset_name: str) -> Dict[str, Any]:
        """Retrieve the latest anomaly detection results for a dataset."""
        query = """
            SELECT * FROM data_anomalies
            WHERE dataset_name = ?
            ORDER BY comparison_date DESC
            LIMIT 1
        """
        
        result = self.db.fetch_one(query, (dataset_name,))
        if result:
            return {
                'id': result[0],
                'dataset_name': result[1],
                'comparison_date': result[2],
                'summary': json.loads(result[3]),
                'column_anomalies': json.loads(result[4]),
                'new_values': json.loads(result[5]),
                'missing_values': json.loads(result[6]),
                'created_at': result[7]
            }
        return None
