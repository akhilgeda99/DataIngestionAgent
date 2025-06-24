"""Module for managing business rule storage and retrieval."""
from typing import List, Dict, Any, Optional
import json
import os
from datetime import datetime
from src.database import db

class RuleStorage:
    def __init__(self, upload_dir: str = "uploads/rules"):
        self.upload_dir = upload_dir
        os.makedirs(upload_dir, exist_ok=True)
        
    def save_rules_to_file(self, rules: List[Dict], name: str) -> str:
        """Save business rules to a JSON file.
        
        Args:
            rules: List of rule dictionaries
            name: Base name for the rules file
            
        Returns:
            str: Path to saved rules file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{name}.json"
        filepath = os.path.join(self.upload_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump({'rules': rules}, f, indent=2)
            
        return filepath
    
    def load_rules_from_file(self, filename: str) -> List[Dict]:
        """Load business rules from a JSON file.
        
        Args:
            filename: Name of the rules file
            
        Returns:
            List[Dict]: List of rule dictionaries
        """
        filepath = os.path.join(self.upload_dir, filename)
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data.get('rules', [])
    
    def save_rules_to_db(self, rules: List[Dict], name: str, 
                        description: Optional[str] = None,
                        database: Optional[str] = None) -> int:
        """Save business rules to database.
        
        Args:
            rules: List of rule dictionaries
            name: Name of the rule set
            description: Optional description
            database: Optional database name
            
        Returns:
            int: ID of saved rule set
        """
        # Create business_rules table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS business_rules (
            id INTEGER PRIMARY KEY IDENTITY(1,1),
            name VARCHAR(255) NOT NULL,
            description VARCHAR(1000),
            rules NVARCHAR(MAX) NOT NULL,
            created_at DATETIME DEFAULT GETDATE(),
            updated_at DATETIME DEFAULT GETDATE()
        )
        """
        db.execute_sql(create_table_sql, database=database)
        
        # Insert rules
        insert_sql = """
        INSERT INTO business_rules (name, description, rules)
        VALUES (?, ?, ?)
        """
        params = (name, description, json.dumps({'rules': rules}))
        return db.execute_sql(insert_sql, params=params, database=database)
    
    def load_rules_from_db(self, rule_id: int, database: Optional[str] = None) -> List[Dict]:
        """Load business rules from database.
        
        Args:
            rule_id: ID of the rule set
            database: Optional database name
            
        Returns:
            List[Dict]: List of rule dictionaries
        """
        select_sql = "SELECT rules FROM business_rules WHERE id = ?"
        result = db.execute_sql(select_sql, params=(rule_id,), 
                              database=database, fetch='one')
        if result and result[0]:
            return json.loads(result[0]).get('rules', [])
        return []
    
    def list_rule_sets(self, database: Optional[str] = None) -> List[Dict]:
        """List all available rule sets.
        
        Args:
            database: Optional database name
            
        Returns:
            List[Dict]: List of rule set metadata
        """
        select_sql = """
        SELECT id, name, description, created_at, updated_at 
        FROM business_rules
        ORDER BY updated_at DESC
        """
        results = db.execute_sql(select_sql, database=database, fetch='all')
        return [
            {
                'id': row[0],
                'name': row[1],
                'description': row[2],
                'created_at': row[3].isoformat() if row[3] else None,
                'updated_at': row[4].isoformat() if row[4] else None
            }
            for row in results
        ]
