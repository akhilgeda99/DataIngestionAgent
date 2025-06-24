"""Database configuration and utilities.
Provides database connection and query functionality with support for multiple databases.
"""
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import yaml
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from urllib.parse import quote_plus

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize SQLAlchemy base class for models
Base = declarative_base()

class Database:
    """Main database handler class that manages connections and queries."""
    
    def __init__(self):
        """Initialize database connections from config file."""
        self.engines = {}  # Store database engines
        self.sessions = {}  # Store database sessions
        self.configs = self._load_config()
        
        # Print connection URL for default database
        default_db = self._get_default_database()
        config = self.configs[default_db]
        url = self._build_connection_url(config)
        print(f"\nConnection URL: {url}\n")
    
    def _load_config(self) -> Dict:
        """Load database configurations from YAML file.
        
        Returns:
            Dict containing database configurations
        """
        try:
            config_path = Path(__file__).parent.parent / 'config' / 'database.yaml'
            with open(config_path) as f:
                config = yaml.safe_load(f)
            
            # Get default database config
            default_db = config.get('default')
            if not default_db or default_db not in config['databases']:
                raise ValueError("Default database not specified or invalid")
                
            db_config = config['databases'][default_db]
            # Print connection details
            print("\nDatabase Connection Details:")
            print(f"Type: {db_config['type']}")
            print(f"Host: {db_config['host']}")
            print(f"Database: {db_config['database']}")
            print(f"Driver: {db_config.get('driver', 'Not specified')}")
            print(f"Trusted Connection: {db_config.get('trusted_connection', False)}\n")
            
            return config['databases']
            
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def get_connection(self, database: Optional[str] = None) -> Any:
        """Get a database connection.
        
        Args:
            database: Name of database to connect to (e.g. 'postgresql', 'mysql')
                     If None, uses default from config
        
        Returns:
            SQLAlchemy engine for database connection
        """
        if database is None:
            database = self._get_default_database()
        
        if database not in self.engines:
            if database not in self.configs:
                raise ValueError(f"Database '{database}' not found in config")
            
            config = self.configs.get(database, {})
            if not config:
                raise ValueError(f"Empty configuration for database '{database}'")
            
            url = self._build_connection_url(config)
            logger.info(f"Connection URL: {url}")
            
            self.engines[database] = create_engine(
                url,
                pool_size=config.get('pool_size', 5),
                max_overflow=config.get('max_overflow', 10),
                pool_timeout=config.get('pool_timeout', 30)
            )
        
        return self.engines[database]
    
    def get_session(self, database: Optional[str] = None):
        """Get a database session for queries.
        
        Args:
            database: Name of database to connect to
        
        Returns:
            SQLAlchemy session
        """
        engine = self.get_connection(database)
        
        if database not in self.sessions:
            self.sessions[database] = sessionmaker(bind=engine)
            
        return self.sessions[database]()
    
    def _get_default_database(self) -> str:
        """Get name of default database from config."""
        # Use already loaded config instead of loading again
        if hasattr(self, 'configs'):
            return self.configs.get('default', 'mssql')
        
        config_path = Path(__file__).parent.parent / 'config' / 'database.yaml'
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        return config.get('default', 'mssql')
    
    def _build_connection_url(self, config: Dict) -> str:
        """Build SQLAlchemy connection URL from config.
        
        Args:
            config: Database configuration dictionary
            
        Returns:
            Connection URL string
        """
        db_type = config['type']
        
        if db_type == 'mssql':
            # SQL Server connection string
            params = []
            if config.get('trusted_connection'):
                params.append('Trusted_Connection=yes')
            if config.get('driver'):
                driver = quote_plus(config['driver'])
                params.append(f"driver={driver}")
            
            url = f"mssql+pyodbc://{config['host']}/{config['database']}"
            if params:
                url += '?' + '&'.join(params)
            return url
            
        # Build base URL with username/password if provided
        if config.get('username') and config.get('password'):
            url = f"{db_type}://{config['username']}:{config['password']}@"
        else:
            url = f"{db_type}://"
            
        # Add host and port if specified
        url += config['host']
        if config.get('port'):
            url += f":{config['port']}"
            
        # Add database name
        url += f"/{config['database']}"
        
        # Add any additional parameters
        params = config.get('additional_params', {})
        if params:
            param_str = '&'.join(f"{k}={v}" for k, v in params.items())
            url += f"?{param_str}"
            
        return url
        
    def get_engine(self):
        """Get the default database engine."""
        default_db = self._get_default_database()
        if default_db not in self.engines:
            config = self.configs[default_db]
            url = self._build_connection_url(config)
            self.engines[default_db] = create_engine(url)
        return self.engines[default_db]

    def list_tables(self, database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict]:
        """List all tables in the database.
        
        Args:
            database: Name of database to query
            schema: Optional schema name to filter tables
            
        Returns:
            List of tables with their schemas
        """
        engine = self.get_connection(database)
        inspector = inspect(engine)
        
        logger.info("Listing tables in database...")
        tables = []
        schema_names = inspector.get_schema_names()
        logger.info(f"Found {len(schema_names)} schemas: {schema_names}")
        
        if schema:
            schema_names = [s for s in schema_names if s == schema]
        
        for schema_name in schema_names:
            for table_name in inspector.get_table_names(schema=schema_name):
                tables.append({
                    'schema': schema_name,
                    'table': table_name
                })
                
        return tables
        
    def get_table_info(self, table_name: str, database: Optional[str] = None) -> Dict:
        """Get table metadata including columns and types.
        
        Args:
            table_name: Name of table (with optional schema)
            database: Name of database to query
            
        Returns:
            Dictionary with table metadata
        """
        engine = self.get_connection(database)
        inspector = inspect(engine)
        
        # Split schema and table name
        parts = table_name.split('.')
        if len(parts) == 2:
            schema, table = parts
        else:
            schema = 'dbo'  # Default schema for SQL Server
            table = parts[0]
            
        columns = []
        for col in inspector.get_columns(table, schema=schema):
            columns.append({
                'name': col['name'],
                'type': str(col['type']),
                'nullable': col['nullable']
            })
            
        return {
            'schema': schema,
            'name': table,
            'columns': columns
        }
        
    def read_table(self, table_name: str, schema: str = 'dbo',
                   database: Optional[str] = None) -> pd.DataFrame:
        """Read data from a table into a pandas DataFrame.
        
        Args:
            table_name: Name of table to read
            schema: Schema name (default: dbo)
            database: Name of database to query
            
        Returns:
            DataFrame containing table data
        """
        engine = self.get_connection(database)
        query = f"SELECT * FROM {schema}.{table_name}"
        return pd.read_sql(text(query), engine)

# Global database instance
db = Database()
