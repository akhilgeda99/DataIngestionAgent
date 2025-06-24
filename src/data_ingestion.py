"""
Data ingestion module for handling file uploads to database.
"""
from src.database import db
import pandas as pd
import logging
from pathlib import Path
from typing import Optional
from sqlalchemy import text

logger = logging.getLogger(__name__)

def ingest_csv_to_sql(
    csv_path: str,
    table_name: str,
    if_exists: str = 'replace',
    index: bool = False,
    schema: Optional[str] = None
) -> bool:
    """
    Ingest data from a CSV file into a SQL Server table.
    
    Args:
        csv_path: Path to the CSV file
        table_name: Name of the target SQL table
        if_exists: How to behave if table exists ('fail', 'replace', or 'append')
        index: Whether to write DataFrame index as a column
        schema: Database schema name (optional)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Verify file exists
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        # Read CSV file
        logger.info(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Get default database engine
        engine = db.get_engine()
        
        # Upload to SQL Server
        logger.info(f"Uploading data to table: {table_name}")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=index,
            schema=schema
        )
        
        logger.info(f"Successfully uploaded {len(df)} rows to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error ingesting CSV data: {str(e)}")
        return False

def read_from_sql(table_name: str, schema: Optional[str] = None) -> pd.DataFrame:
    """
    Read data from SQL Server table into a pandas DataFrame.
    
    Args:
        table_name: Name of the SQL table
        schema: Database schema name (optional)
    
    Returns:
        pd.DataFrame containing the table data
    """
    try:
        engine = db.get_engine()
        query = f"SELECT * FROM {table_name}"
        if schema:
            query = f"SELECT * FROM {schema}.{table_name}"
            
        logger.info(f"Reading data from table: {table_name}")
        df = pd.read_sql(query, engine)
        logger.info(f"Successfully read {len(df)} rows from {table_name}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading from SQL table: {str(e)}")
        raise

if __name__ == "__main__":
    # Example usage
    csv_file = "data/car_sales.csv"
    success = ingest_csv_to_sql(csv_file, "CAR_SALES")
    if success:
        print("Data ingestion completed successfully")
    else:
        print("Data ingestion failed")
