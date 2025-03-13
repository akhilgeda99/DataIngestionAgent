"""Database configuration and utilities.

This module provides SQLAlchemy configuration and database utility functions.
"""
from sqlalchemy import create_engine, MetaData, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from typing import Dict, Any, List
import urllib.parse
import logging
import pyodbc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQL Server configuration
SERVER = "AKHILSALIENWARE"
DATABASE = "AI_AGENT"
DRIVER = "ODBC Driver 17 for SQL Server"  # Using the ODBC Driver 17
TRUSTED_CONNECTION = "yes"

logger.info(f"Connecting to SQL Server: {SERVER}, Database: {DATABASE}")

# Create connection string
params = urllib.parse.quote_plus(
    f'DRIVER={{{DRIVER}}};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'Trusted_Connection={TRUSTED_CONNECTION};'
)

SQLALCHEMY_DATABASE_URL = f"mssql+pyodbc:///?odbc_connect={params}"
logger.info(f"Connection URL: {SQLALCHEMY_DATABASE_URL}")

try:
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        fast_executemany=True
    )
    # Test connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DB_NAME()")).scalar()
        logger.info(f"Successfully connected to database: {result}")
except Exception as e:
    logger.error(f"Error connecting to database: {str(e)}")
    raise

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Get database session.
    
    Yields:
        Session: Database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_table_metadata(table_name: str) -> Dict[str, Any]:
    """Get metadata for a specific table.
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        Dict[str, Any]: Table metadata including columns and types
    """
    try:
        inspector = inspect(engine)
        if '.' in table_name:
            schema, table = table_name.split('.')
        else:
            schema = 'dbo'  # default schema
            table = table_name
            
        if not inspector.has_table(table, schema=schema):
            raise ValueError(f"Table {schema}.{table} not found")
            
        columns = inspector.get_columns(table, schema=schema)
        return {
            "table_name": table,
            "schema": schema,
            "columns": [
                {"name": col["name"], "type": str(col["type"])}
                for col in columns
            ]
        }
    except Exception as e:
        logger.error(f"Error getting table metadata: {str(e)}")
        raise

def read_table_data(table_name: str, schema: str = None) -> pd.DataFrame:
    """Read data from a database table into a pandas DataFrame.
    
    Args:
        table_name (str): Name of the table to read
        schema (str, optional): Schema name. If None, uses default schema.
        
    Returns:
        pd.DataFrame: Table data as a DataFrame
        
    Raises:
        ValueError: If table doesn't exist
    """
    try:
        schema = schema or 'dbo'
        
        # Verify table exists before querying
        inspector = inspect(engine)
        if not inspector.has_table(table_name, schema=schema):
            raise ValueError(f"Table [{schema}].[{table_name}] does not exist")
            
        query = text(f"SELECT TOP 1000 * FROM [{schema}].[{table_name}]")
        logger.info(f"Executing query: {query}")
        return pd.read_sql(query, engine)
    except ValueError as ve:
        logger.error(f"Table validation error: {str(ve)}")
        raise
    except Exception as e:
        logger.error(f"Error reading table data: {str(e)}")
        raise

def list_tables(schema: str = None) -> List[Dict[str, Any]]:
    """List all tables in the database.
    
    Args:
        schema (str, optional): Schema name. If None, lists tables from all schemas.
    
    Returns:
        List[Dict[str, Any]]: List of tables with their schemas
    """
    try:
        inspector = inspect(engine)
        tables = []
        
        if schema:
            schemas = [schema]
        else:
            # Only get user schemas, excluding system schemas
            all_schemas = inspector.get_schema_names()
            schemas = [s for s in all_schemas if s not in ('sys', 'INFORMATION_SCHEMA', 'guest', 
                'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 
                'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 
                'db_denydatawriter')]
            logger.info(f"Found user schemas: {schemas}")
        
        for schema_name in schemas:
            try:
                table_names = inspector.get_table_names(schema=schema_name)
                logger.info(f"Found tables in schema {schema_name}: {table_names}")
                for table_name in table_names:
                    tables.append({
                        "schema": schema_name,
                        "table": table_name
                    })
            except Exception as schema_error:
                logger.warning(f"Error accessing schema {schema_name}: {str(schema_error)}")
                continue
        
        if not tables:
            logger.warning("No accessible tables found in the database")
            
        return tables
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        raise
