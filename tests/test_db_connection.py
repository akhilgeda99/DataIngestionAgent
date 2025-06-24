"""Test suite for MSSQL database connection functionality."""
import pytest
import yaml
from pathlib import Path
import sys
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

# Add src directory to Python path
src_dir = str(Path(__file__).parent.parent / 'src')
if src_dir not in sys.path:
    sys.path.append(src_dir)

from database import Database

# Test configuration for MSSQL only
TEST_CONFIG = {
    'default': 'mssql',
    'databases': {
        'mssql': {
            'type': 'mssql',
            'host': 'AKHILSALIENWARE',
            'database': 'AI_AGENT',
            'driver': 'ODBC Driver 17 for SQL Server',
            'trusted_connection': True,
            'pool_size': 5,
            'max_overflow': 10,
            'pool_timeout': 30,
            'pool_recycle': 1800
        }
    }
}

@pytest.fixture
def mock_yaml_load():
    """Mock yaml.safe_load to return our test configuration."""
    with patch('yaml.safe_load', return_value=TEST_CONFIG):
        yield TEST_CONFIG

@pytest.fixture
def test_database(mock_yaml_load):
    """Create a test database instance with mocked configuration."""
    return Database()

def test_load_config(test_database):
    """Test loading database configuration."""
    configs = test_database.configs
    assert configs == TEST_CONFIG['databases']
    assert 'mssql' in configs
    assert configs['mssql']['type'] == 'mssql'
    assert configs['mssql']['host'] == 'AKHILSALIENWARE'
    assert configs['mssql']['database'] == 'AI_AGENT'

def test_get_default_database(test_database):
    """Test getting default database name."""
    default_db = test_database._get_default_database()
    assert default_db == 'mssql'
    assert default_db == TEST_CONFIG['default']

def test_build_mssql_connection_url(test_database):
    """Test building MSSQL connection URL."""
    config = TEST_CONFIG['databases']['mssql']
    url = test_database._build_connection_url(config)
    
    # Basic URL components
    assert url.startswith('mssql+pyodbc://')
    assert 'AKHILSALIENWARE' in url
    assert 'AI_AGENT' in url
    
    # Parameters
    assert 'Trusted_Connection=yes' in url
    driver = quote_plus('ODBC Driver 17 for SQL Server')
    assert f'driver={driver}' in url

@patch('sqlalchemy.create_engine')
def test_get_connection_success(mock_create_engine, test_database):
    """Test successful database connection."""
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    
    # Test getting default connection (MSSQL)
    engine = test_database.get_connection()
    assert engine == mock_engine
    mock_create_engine.assert_called_once()
    
    # Test connection pooling parameters
    call_kwargs = mock_create_engine.call_args[1]
    assert call_kwargs['pool_size'] == 5
    assert call_kwargs['max_overflow'] == 10
    assert call_kwargs['pool_timeout'] == 30

def test_get_connection_invalid_database(test_database):
    """Test getting connection with invalid database name."""
    with pytest.raises(ValueError) as exc_info:
        test_database.get_connection('invalid_db')
    assert "Database 'invalid_db' not found in config" in str(exc_info.value)

@patch('sqlalchemy.create_engine')
def test_get_connection_error(mock_create_engine, test_database):
    """Test database connection error handling."""
    mock_create_engine.side_effect = SQLAlchemyError("Connection failed")
    
    with pytest.raises(SQLAlchemyError) as exc_info:
        test_database.get_connection()
    assert "Connection failed" in str(exc_info.value)

@patch('sqlalchemy.create_engine')
def test_connection_reuse(mock_create_engine, test_database):
    """Test that connections are reused for the same database."""
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    
    # Get connection twice for same database
    engine1 = test_database.get_connection('mssql')
    engine2 = test_database.get_connection('mssql')
    
    # Verify same engine is returned and create_engine called only once
    assert engine1 == engine2
    mock_create_engine.assert_called_once()

def test_get_session(test_database):
    """Test getting database session."""
    with patch('sqlalchemy.orm.sessionmaker') as mock_sessionmaker:
        mock_session = MagicMock()
        mock_sessionmaker.return_value = lambda: mock_session
        
        session = test_database.get_session()
        assert session == mock_session

def test_connection_pool_settings(test_database):
    """Test that connection pool settings are correctly applied."""
    with patch('sqlalchemy.create_engine') as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        test_database.get_connection('mssql')
        
        # Verify pool settings from config are used
        call_kwargs = mock_create_engine.call_args[1]
        config = TEST_CONFIG['databases']['mssql']
        assert call_kwargs['pool_size'] == config['pool_size']
        assert call_kwargs['max_overflow'] == config['max_overflow']
        assert call_kwargs['pool_timeout'] == config['pool_timeout']

if __name__ == '__main__':
    pytest.main(['-v', __file__])
