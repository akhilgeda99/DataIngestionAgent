"""Test script for database connectivity and table listing."""
import sys
from pathlib import Path
import logging
from typing import List, Dict

# Add src directory to Python path
src_dir = str(Path(__file__).parent.parent / 'src')
if src_dir not in sys.path:
    sys.path.append(src_dir)

from database import db

def test_database_connection():
    """Test database connection and list all tables."""
    try:
        print("\n=== Testing Database Connection ===")
        
        # Get default database connection
        default_db = db._get_default_database()
        engine = db.get_connection(default_db)
        print("✓ Database connection successful")
        
        # List all tables
        print("\n=== Listing All Tables ===")
        tables = db.list_tables(default_db)
        if not tables:
            print("No tables found in database")
        else:
            print(f"Found {len(tables)} tables:")
            for table_info in tables:
                schema = table_info['schema']
                table_name = table_info['table']
                print(f"- [{schema}].[{table_name}]")
                
                # Get table info
                print("\nTable Details:")
                info = db.get_table_info(f"{schema}.{table_name}", database=default_db)
                print("Columns:")
                for col in info['columns']:
                    print(f"  - {col['name']} ({col['type']})")
                print()
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        logging.exception("Error in database test")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    test_database_connection()
