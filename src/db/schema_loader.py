import os
import json
from typing import Dict, Any, List
from pathlib import Path

from src.db.database import Database


class SchemaLoader:
    """Utility to load and process database schema information"""
    
    def __init__(self, db: Database, schema_dir: str = "data/schema"):
        self.db = db
        self.schema_dir = schema_dir
        self.schema = {}
        self.ensure_schema_dir()
    
    def ensure_schema_dir(self):
        """Ensure the schema directory exists"""
        Path(self.schema_dir).mkdir(parents=True, exist_ok=True)
    
    def load_schema_from_db(self) -> Dict[str, Any]:
        """Load schema information from the database"""
        self.schema = self.db.get_db_schema()
        return self.schema
    
    def save_schema_to_file(self, filename: str = "schema.json"):
        """Save schema information to a JSON file"""
        file_path = os.path.join(self.schema_dir, filename)
        with open(file_path, 'w') as f:
            json.dump(self.schema, f, indent=2)
        print(f"Schema saved to {file_path}")
    
    def load_schema_from_file(self, filename: str = "schema.json") -> Dict[str, Any]:
        """Load schema information from a JSON file"""
        file_path = os.path.join(self.schema_dir, filename)
        try:
            with open(file_path, 'r') as f:
                self.schema = json.load(f)
            print(f"Schema loaded from {file_path}")
            return self.schema
        except FileNotFoundError:
            print(f"Schema file {file_path} not found. Loading from database.")
            return self.load_schema_from_db()
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        if not self.schema:
            self.load_schema_from_db()
        
        if table_name in self.schema:
            return self.schema[table_name]['columns']
        else:
            print(f"Table {table_name} not found in schema")
            return []
    
    def get_table_relationships(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get relationships between tables based on foreign keys"""
        if not self.schema:
            self.load_schema_from_db()
        
        relationships = {}
        
        for table_name, table_info in self.schema.items():
            relationships[table_name] = []
            
            for fk in table_info['foreign_keys']:
                relationship = {
                    'source_table': table_name,
                    'source_column': fk['column_name'],
                    'target_table': fk['foreign_table_name'],
                    'target_column': fk['foreign_column_name']
                }
                relationships[table_name].append(relationship)
        
        return relationships
    
    def generate_join_paths(self) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Generate possible join paths between tables"""
        if not self.schema:
            self.load_schema_from_db()
        
        join_paths = {}
        
        # Get all direct relationships
        relationships = self.get_table_relationships()
        
        # For each table, find all possible join paths to other tables
        for source_table in self.schema.keys():
            join_paths[source_table] = {}
            
            for target_table in self.schema.keys():
                if source_table == target_table:
                    continue
                
                join_paths[source_table][target_table] = self._find_join_path(source_table, target_table, relationships)
        
        return join_paths
    
    def _find_join_path(self, source_table: str, target_table: str, 
                       relationships: Dict[str, List[Dict[str, Any]]], 
                       visited: List[str] = None, path: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Find a join path between two tables using a breadth-first search"""
        if visited is None:
            visited = []
        if path is None:
            path = []
        
        if source_table == target_table:
            return path
        
        visited.append(source_table)
        
        # Check direct relationships
        for rel in relationships.get(source_table, []):
            next_table = rel['target_table']
            
            if next_table == target_table:
                return path + [rel]
            
            if next_table not in visited:
                new_path = self._find_join_path(next_table, target_table, relationships, visited, path + [rel])
                if new_path:
                    return new_path
        
        return []


def main():
    """Main function to load and save database schema"""
    db = Database()
    loader = SchemaLoader(db)
    schema = loader.load_schema_from_db()
    loader.save_schema_to_file()
    
    # Generate and save join paths
    join_paths = loader.generate_join_paths()
    with open(os.path.join(loader.schema_dir, "join_paths.json"), 'w') as f:
        json.dump(join_paths, f, indent=2)
    
    print("Schema and join paths saved successfully")
    db.disconnect()


if __name__ == "__main__":
    main() 