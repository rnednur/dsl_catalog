import os
from typing import Any, Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


class Database:
    """PostgreSQL database connection and operations"""
    
    def __init__(self):
        self.conn = None
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'dbname': os.getenv('DB_NAME', 'nl2sql'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
    
    def connect(self):
        """Connect to the PostgreSQL database"""
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(**self.db_config)
                print("Connected to PostgreSQL database")
            except Exception as e:
                print(f"Error connecting to PostgreSQL database: {e}")
                raise
        return self.conn
    
    def disconnect(self):
        """Disconnect from the PostgreSQL database"""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            print("Disconnected from PostgreSQL database")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return the results as a list of dictionaries"""
        conn = self.connect()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params or ())
            if cursor.description:
                results = cursor.fetchall()
                return [dict(row) for row in results]
            else:
                conn.commit()
                return []
    
    def get_tables(self) -> List[str]:
        """Get list of tables in the database"""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        """
        results = self.execute_query(query)
        return [result['table_name'] for result in results]
    
    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get list of columns for a table"""
        query = """
        SELECT 
            column_name, 
            data_type, 
            is_nullable,
            column_default
        FROM 
            information_schema.columns
        WHERE 
            table_schema = 'public' AND 
            table_name = %s
        ORDER BY 
            ordinal_position
        """
        return self.execute_query(query, (table_name,))
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary keys for a table"""
        query = """
        SELECT 
            kcu.column_name
        FROM 
            information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
        WHERE 
            tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
            AND tc.table_name = %s
        ORDER BY 
            kcu.ordinal_position
        """
        results = self.execute_query(query, (table_name,))
        return [result['column_name'] for result in results]
    
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign keys for a table"""
        query = """
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
            AND tc.table_name = %s
        """
        return self.execute_query(query, (table_name,))
    
    def get_db_schema(self) -> Dict[str, Any]:
        """Get complete schema information for the database"""
        tables = self.get_tables()
        schema = {}
        
        for table in tables:
            columns = self.get_columns(table)
            primary_keys = self.get_primary_keys(table)
            foreign_keys = self.get_foreign_keys(table)
            
            schema[table] = {
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys
            }
        
        return schema 